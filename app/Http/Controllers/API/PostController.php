<?php

namespace App\Http\Controllers\API;

use App\Forum;
use App\Http\Controllers\Controller;
use App\Jobs\SQS\PushForumId;
use App\Jobs\SQS\PushPostId;
use App\Jobs\SQS\PushRevisionId;
use App\Jobs\SQS\PushWikidotUserId;
use Illuminate\Http\Request;
use App\Post;
use App\Thread;
use App\WikidotUser;
use Carbon\Carbon;
use App\Domain;
use Illuminate\Support\Facades\Gate;
use Illuminate\Support\Facades\Log;

class PostController extends Controller
{
    public function put_thread_posts(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            $t = Thread::where('wd_thread_id', $request["wd_thread_id"])->get();
            if($t->isEmpty()) {
                // Well this is awkward.
                // 2stacks just sent us posts in a thread we don't have.
                // Summon the troops.
                Log::error('2stacks sent us a thread id for ' . $request["wd_thread_id"]. ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching thread!');
                Log::error('$request: ' . $request);
                return response('I don\'t have a thread to attach these posts to!', 500)
                    ->header('Content-Type', 'text/plain');
            }
            else {
                $thread = $t->first();
                $metadata = json_decode($thread->metadata, true);
                if(isset($metadata["thread_missing_posts"]) && $metadata["thread_missing_posts"] == true) {
                    // This is our typical use case for this call.
                    // Let's unpack some metadata and store it before we get into posts.
                    $thread->wd_forum_id = $request["wd_forum_id"];

                    // Let's see if we have this forum already.
                    $f = Forum::where('wd_forum_id', $request["wd_forum_id"])->get();
                    if($f->isEmpty()) {
                        $forum = new Forum([
                            'wd_forum_id' => $request["wd_forum_id"],
                            'wiki_id' => $domain->wiki->id,
                            'metadata' => json_encode(array("forum_needs_metadata" => true)),
                            'jsontimestamp' => Carbon::now()
                        ]);
                        $forum->save();
                    }
                    else { $forum = $f->first(); }

                    $thread->forum_id = $forum->id;

                    // More metadata getting
                    $thread->wd_user_id = $request["wd_user_id"];
                    $thread->title = $request["title"];
                    $thread->subtitle = $request["subtitle"];
                    $metadata["wd_created_at"] = $request["created_at"];
                    $thread->jsontimestamp = Carbon::now();
                    $thread->metadata = json_encode($metadata);
                    $thread->save();

                    // Let's unpack our posts and save them one at a time.
                    foreach($request["posts"] as $post) {
                        $result = $this->write_post($post, $thread, $domain);
                    }

                    // Wrap up now that we're done with at least the first page of posts.
                    unset($metadata["thread_missing_posts"]);
                    $thread->jsontimestamp = Carbon::now();
                    $thread->metadata = json_encode($metadata);
                    $thread->save();
                }
                else {
                    // We will get lots of responses back for a particular thread full of posts so this will happen plenty as well.
                    // We will not need to store thread metadata in this routine but we will need to check for duplicates.
                    foreach($request["posts"] as $post) {
                        $dupe = Post::where('wd_post_id', $post["wd_post_id"])->get();
                        if($dupe->isEmpty()) {
                            $result = $this->write_post($post, $thread, $domain);
                        }
                    }
                }
            return "saved";
            }
        }
    }

    public function write_post(array $p, Thread $thread, Domain $domain)
    {
        $post = new Post([
            'thread_id' => $thread->id,
            'forum_id' => $thread->forum_id,
            'wiki_id' => $domain->wiki_id,
            'user_id' => auth()->id(),
            'wd_user_id' => $p["wd_user_id"],
            'subject' => $p["subject"],
            'text' => $p["text"],
            'wd_post_id' => $p["wd_post_id"]
        ]);
        // Figure out the parent ID.
        if($p["parent_id"] == 0) {
            $post->wd_parent_id = 0;
            $post->parent_id = 0;
        }
        else {
            // We need to avoid a race condition here.
            // It's possible that these posts are handled out of order, delivery is eventually reliable.
            // Let's hope for the best first.
            $parent = Post::where('wd_post_id', $p["parent_id"])->get();
            if($parent->isEmpty() == false) {
                $op = $parent->first();
                $post->wd_parent_id = $p["parent_id"];
                $post->parent_id = $op->id;
            }
            else {
                // Now we've got a claimed parent ID that is not yet in the table.
                // We'll create an intentional mismatch that we will handle on a schedule within SCUTTLE.
                $post->wd_parent_id = $p["parent_id"];
                $post->parent_id = 0;
            }
        }
        // Initialize post metadata
        $pm = array();

        // A post may have been edited. We'll handle this in a different routine.
        if(isset($p["changes"]) && $p["changes"] != false) {
            foreach($p["changes"] as $revision) {
                $pm["revisions"][]["wd_revision_id"] = $revision;

                $job = new PushPostId($revision["revisions"], $domain->wiki->id);
                $job->send('scuttle-posts-missing-revisions');
            }
        }

        // Metadata
        $pm["wd_username"] = $p["username"];
        $pm["wd_timestamp"] = $p["timestamp"];
        $post->metadata = json_encode($pm);
        $post->jsontimestamp = Carbon::now();
        // That should be everything. Let's save the post.
        $post->save();

        return true;
    }
}
