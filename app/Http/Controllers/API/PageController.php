<?php

namespace App\Http\Controllers\API;

use App\Domain;
use App\File;
use App\Jobs\SQS\PushPageId;
use App\Jobs\SQS\PushPageSlug;
use App\Jobs\SQS\PushThreadId;
use App\Jobs\SQS\PushWikidotUserId;
use App\Page;
use App\Revision;
use App\Thread;
use App\Vote;
use App\WikidotUser;
use Illuminate\Http\Request;
use App\Http\Controllers\Controller;
use Carbon\Carbon;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Gate;

class PageController extends Controller
{
    public function put_2stacks_pages_manifest(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            Log::debug('In the manifest for ' . $domain->domain);
            $reportedpages = $request->toArray(); // Wikidot's list of pages as provided by 2stacks/lambda.
            $scuttlepages = Page::where('wiki_id',$domain->wiki_id)
                ->pluck('slug')
                ->toArray(); // Our list of pages which does *not* include trashed/deleted pages.

            // This is an alternative to array_diff that works about 1400x faster.
            // It will find a single mismatch in a pair of 11,000+ item lists in under 2 seconds on a single core.
            // https://stackoverflow.com/a/6700430/3946227
            function leo_array_diff($a, $b) {
                $map = array();
                foreach($a as $val) $map[$val] = 1;
                foreach($b as $val) unset($map[$val]);
                return array_keys($map);
            }
            Log::debug('Running the array diff...');
            $unaccountedpages = leo_array_diff($reportedpages, $scuttlepages);

            if(empty($scuttlepages)) {
                // We're working with an empty set, either because of a rollback or because we're tracking a new wiki for the first time.
                $unaccountedpages = $reportedpages;
            }
            Log::debug('unaccountedpages: ' . var_dump($unaccountedpages));
            if(count($unaccountedpages) > 0) {
                // Ping Discord.
                $wd_url = (json_decode($domain->wiki->metadata, true))["wd_url"];
                if(count($unaccountedpages) === 1) {
                    discord(
                        'page-new',
                        "Received slug `".$unaccountedpages[0]."` for domain `".$domain->domain."`, dispatching jobs.\n[SCUTTLE](https://".$domain->domain."/".$unaccountedpages[0].") | [Wikidot](http://".$wd_url."/".$unaccountedpages[0].")"
                    );
                }
                else {
                    $urls = "\n";
                    foreach($unaccountedpages as $p) {
                        $urls .= "• `".$p."`: [SCUTTLE](https://".$domain->domain."/".$p.") | [Wikidot](http://".$wd_url."/".$p.")\n";
                    }
                    if(strlen($urls) > 5000) { $urls = "\nThat's a whole bunch.\n"; }
                    discord(
                        'page-new',
                        "Received ".count($unaccountedpages)." slugs for domain `" . $domain->domain . "`\n".$urls."\nDispatching jobs. <a:workwork:674436294708953109>"
                    );
                }
            }
            // Let's stub out the page and note that we need metadata for the page.
            foreach ($unaccountedpages as $item) {
                Log::debug('Processing ' . $item . '...');
                $lastmilestone = Page::withTrashed()->where('wiki_id',$domain->wiki_id)->where('slug',$item)->orderBy('milestone','desc')->pluck('milestone')->first();
                if ($lastmilestone === null) { $milestone = 0; }
                else { $milestone = $lastmilestone + 1; }
                $page = new Page([
                    'wiki_id' => $domain->wiki->id,
                    'user_id' => auth()->id(),
                    'slug' => $item,
                    'milestone' => $milestone,
                    'metadata' => json_encode(
                        array(
                            'page_missing_metadata' => true
                        )
                    ),
                    'JsonTimestamp' => Carbon::now()
                ]);
                $page->save();
                Log::debug('Saved, pushing SQS job.');
                // Send an SQS message for 2stacks-lambda to work on.
                $job = new PushPageSlug($page->slug, $domain->wiki_id);
                $job->send('scuttle-pages-missing-metadata');
                Log::debug('Job sez:' . var_dump($job));
            }
            // Now, we can also infer pages that have been deleted by taking the opposite diff.
            $deletedpages = leo_array_diff($scuttlepages, $reportedpages);
            Log::debug('Deleted pages: ' . var_dump($deletedpages));

            if(count($deletedpages) > 0) {
                $fifostring = bin2hex(random_bytes(64));
                // Ping Discord.
                if(count($deletedpages) === 1) {
                    discord(
                        'page-missing',
                        "Slug `".$deletedpages[0]."` for domain `".$domain->domain."` not present in manifest, dispatching job ending in `".substr($fifostring,-16)."`.\n[SCUTTLE](https://".$domain->domain."/".$deletedpages[0].") | [Wikidot](http://".(json_decode($domain->wiki->metadata, true))["wd_url"]."/".$deletedpages[0].")"
                    );
                }
                else {
                    discord(
                        'page-missing',
                        "".count($deletedpages)." slugs for domain `" . $domain->domain . "` not present in manifest, dispatching job ending in `".substr($fifostring,-16)."`."
                    );
                }
            }

            foreach($deletedpages as $deletedpage) {
                // We need to determine whether the page was actually deleted or renamed, we have a lambda for that.
                $page = Page::where('wiki_id', $domain->wiki_id)->where('slug', $deletedpage)->orderBy('milestone','desc')->first();

                // If we haven't yet received a Wikidot page ID, and it's missing from the manifest, let's give it one
                // iteration to show up, then delete it. The lambda can't work with a null ID.
                if($page->wd_page_id == null) {
                    $metadata = json_decode($page->metadata, true);
                    if(isset($metadata["page_missing"]) && $metadata["page_missing"] == true) {
                        discord(
                            'page-deleted',
                            "Deleting `".$page->slug."` (SCUTTLE ID `".$page->id."`) after it was flagged missing without a Wikidot page ID to reference."
                        );

                        $page->delete();
                    }
                    else {
                        $metadata["page_missing"] = true;
                        $page->metadata = json_encode($metadata);
                        $page->JsonTimestamp = Carbon::now();
                        $page->save();
                    }
                }
                else {
                    // We're going to add a flag to the page metadata so the page deletion lambda can't be abused.
                    $metadata = json_decode($page->metadata, true);
                    $metadata["page_missing"] = true;
                    $page->metadata = json_encode($metadata);
                    $page->JsonTimestamp = Carbon::now();
                    $page->save();

                    $job = new PushPageId($page->wd_page_id, $domain->wiki_id);
                    $job->send('scuttle-page-check-for-deletion.fifo', $fifostring);
                }
            }
        }
        Log::debug('Leaving!');
    }

    public function sched_pages_metadata(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            // Each request is simple metadata about a single page. We one of these a day for each page.

            // Go get the page first.
            $p = Page::where('wiki_id', $domain->wiki->id)
                ->where('slug', $request["fullname"])
                ->orderBy('milestone', 'desc')
                ->get();

            if($p->isEmpty()) {
                // Counterintuitively, this should never happen. All the slugs we got back were for pages we already had,
                // because we initiated this from the SCUTTLE side.
                // Summon the troops.
                Log::error('2stacks sent us metadata about ' . $request->fullname . ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching slug!');
                Log::error('$request: ' . $request);
                return response('I don\'t have a slug to attach that metadata to!', 500)
                    ->header('Content-Type', 'text/plain');
            }
            else {
                // Back on earth...
                $page = $p->first();

                // We want to make sure our created timestamp matches up, otherwise we're dealing with a new page.
                // We also get a revision count here.
                $metadata = json_decode($page->metadata, true);
                if($metadata["wikidot_metadata"]["created_at"] != $request->created_at) {
                    // New page.
                    $np = new Page([
                        'wiki_id' => $domain->wiki->id,
                        'user_id' => auth()->id(),
                        'slug' => $page->slug,
                        'milestone' => $page->milestone + 1,
                        'latest_revision' => $page->html,
                        'metadata' => json_encode(
                            array(
                                'page_missing_metadata' => true
                            )
                        ),
                        'JsonTimestamp' => Carbon::now()
                    ]);
                    $np->save();
                    // Send an SQS message for 2stacks-lambda to work on.
                    $job = new PushPageSlug($np->slug, $domain->wiki_id);
                    $job->send('scuttle-pages-missing-metadata');
                }
                else {
                    // Let's refresh the wikidot metadata for this page.
                    // These come from pages.get_one so the full set is available to update.
                    // Additionally, we have the rendered latest revision.
                    $metadata["wikidot_metadata"]["updated_at"] = $request->updated_at;
                    $metadata["wikidot_metadata"]["updated_by"] = $request->updated_by;
                    $metadata["wikidot_metadata"]["tags"] = $request->tags;
                    $metadata["wikidot_metadata"]["rating"] = $request->rating;
                    $metadata["wikidot_metadata"]["revisions"] = $request->revisions;
                    $metadata["wikidot_metadata"]["title"] = $request->title;
                    $metadata["wikidot_metadata"]["title_shown"] = $request->title_shown;
                    $metadata["wikidot_metadata"]["parent_fullname"] = $request->parent_fullname;
                    $metadata["wikidot_metadata"]["parent_title"] = $request->parent_title;
                    $metadata["wikidot_metadata"]["children"] = $request->children;
                    $metadata["wikidot_metadata"]["comments"] = $request->comments;
                    $metadata["wikidot_metadata"]["commented_at"] = $request->commented_at;
                    $metadata["wikidot_metadata"]["commented_by"] = $request->commented_by;

                    $page->latest_revision = $request->html;

                    // Does our revision count match? I mean, no, because wikidot has an off-by-one error, but you know.
                    if($request->revisions + 1 != $page->revisions->count()) {
                        // We're missing revisions.
                        $metadata["page_missing_revisions"] = true;
                        // Push the revision gettin' job.
                        $job1 = new PushPageId($page->wd_page_id, $domain->wiki->id);
                        $job1->send('scuttle-pages-missing-revisions');
                    }

                    // Wrap-up.
                    $page->metadata = json_encode($metadata);
                    $page->JsonTimestamp = Carbon::now(); // Touch on update.
                    $page->save();
                }
            }
        }
    }

    public function put_page_metadata(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            // Our page may have been renamed rather than a brand new one. Let's quickly check for that.
            $p = Page::withTrashed()->where('wd_page_id', $request['wd_page_id'])->get();
            if ($p->isNotEmpty()) {
                // Renamed page, let's do the thing.
                $page = $p->first();

                // First off, there's apparently some pages that were incorrectly soft-deleted. Undelete them if so.
                if($page->trashed()) {
                    $page->restore();
                }

                // Ping Discord.
                if($page->slug != $request["slug"]) {
                    discord(
                        'page-moved',
                        "Page with ID `" . $request['wd_page_id'] . "` has been renamed from `" . $page->slug . "` to `" . $request["slug"] . "`. Updating metadata."
                    );
                }
                else {
                    discord(
                        'page-updated',
                        "Page with ID `" . $request['wd_page_id'] . "` (`" . $page->slug . "`) received updated metadata from 2stacks."
                    );
                }
                $metadata = json_decode($page->metadata, true);
                if(isset($metadata["old_slugs"])) {
                    if (in_array($page->slug, $metadata["old_slugs"]) == false) {
                        $metadata["old_slugs"][] = $page->slug;
                    }
                }
                unset($metadata["page_missing"]);
                $page->slug = $request["slug"];
                $page->metadata = json_encode($metadata);
                $page->jsonTimestamp = Carbon::now(); // Touch on update.
                $page->save();
                // Delete the old stubby lad.
                $stubs = Page::where('slug',$request["slug"])->where('wiki_id',$domain->wiki_id)->where('wd_page_id', null)->get();
                foreach($stubs as $stub) {
                    $stub->forceDelete();
                }
                return "renamed page, saved";
            }
            else {
                $p = Page::where('wiki_id', $domain->wiki->id)
                    ->where('slug', $request["slug"])
                    ->orderBy('milestone', 'desc')
                    ->get();
                if ($p->isEmpty()) {
                    // Well this is awkward.
                    // 2stacks just sent us metadata about a slug we don't have.
                    // Summon the troops.
                    Log::error('2stacks sent us metadata about ' . $request->slug . ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching slug!');
                    Log::error('$request: ' . $request);
                    return response('I don\'t have a slug to attach that metadata to!', 500)
                        ->header('Content-Type', 'text/plain');
                } else {
                    $page = $p->first();
                    if(isset($request["api_status"]) && $request["api_status"] == 403) {
                        // This page was blocked from access by the Wikidot API. Annoying.
                        $page->wd_page_id = $request["wd_page_id"];
                        $page->metadata = json_encode(array(
                           'blocked_page' => true
                        ));
                        $page->jsonTimestamp = Carbon::now(); // touch on update
                        $page->save();
                        return "Saved as a blocked page.";
                    }
                    else {
                        $timestamp = Carbon::parse($request["wikidot_metadata"]["created_at"])->timestamp;
                        $oldmetadata = json_decode($page->metadata, true);
                        if (isset($oldmetadata["page_missing_metadata"]) && $oldmetadata["page_missing_metadata"] == true) {
                            // This is the default use case, responding to the initial SQS message on a new page arriving.
                            // SQS queues can send a message more than once so we need to make sure we're handling all possibilities.
                            $page->wd_page_id = $request["wd_page_id"];
                            $page->latest_revision = $request["latest_revision"];
                            $page->metadata = json_encode(array(
                                // We're overwriting the old metadata entirely as the only thing it had was "needs metadata".
                                'wikidot_metadata' => $request["wikidot_metadata"],
                                // We only include these now instead of on initial write because we need the page_id to fire the event.
                                'page_missing_votes' => true,
                                'page_missing_files' => true,
                                'page_missing_revisions' => true,
                                'page_missing_comments' => true,
                                'wd_page_created_at' => $timestamp
                            ));
                            $page->jsonTimestamp = Carbon::now(); // touch on update
                            $page->save();
                            // Go notify the other workers.
                            $job1 = new PushPageId($page->wd_page_id, $domain->wiki->id);
                            $job1->send('scuttle-pages-missing-revisions');
                            $job2 = new PushPageId($page->wd_page_id, $domain->wiki->id);
                            $job2->send('scuttle-pages-missing-thread-id');
                            $job3 = new PushPageSlug($page->slug, $domain->wiki->id);
                            $job3->send('scuttle-pages-missing-files');
                            $job4 = new PushPageId($page->wd_page_id, $domain->wiki->id);
                            $job4->send('scuttle-pages-missing-votes');
                            return response('saved');
                        } else {
                            return response('had that one already');
                        }
                    }
                }
            }
        }
    }

    public function put_page_votes(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            $p = Page::where('wiki_id', $domain->wiki->id)
                ->where('wd_page_id', $request["wd_page_id"])
                ->get();
            if($p->isEmpty()) {
                // Well this is awkward.
                // 2stacks just sent us metadata about a slug we don't have.
                // Summon the troops.
                Log::error('2stacks sent us votes on ' . $request->slug . ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching slug!');
                Log::error('$request: ' . $request);
                return response('I don\'t have a page to attach those votes to!', 500)
                    ->header('Content-Type', 'text/plain');
            }
            else {
                $page = $p->first();
                $oldmetadata = json_decode($page->metadata, true);

                // Get all the existing votes.
                $allvotes = Vote::where('page_id', $page->id)->get();
                // Filter out the old ones, return active, nonmember, and deleted ones.
                $votes = $allvotes->whereNotIn('status','old');
                // Get all the wd_user_ids.
                $oldvoters = $votes->pluck('wd_user_id')->toArray();
                $allvoters = $allvotes->pluck('wd_user_id')->toArray();
                // Make a collection of users.
                $wikidotusers = WikidotUser::whereIn('wd_user_id',$allvoters)->get();
                //Quickly, let's go through the request and pull all the new IDs to an array as we'll need them.
                $newvoters = [];
                foreach ($request["votes"] as $vote) {
                    array_push($newvoters, $vote["user_id"]);
                }
                    foreach($request["votes"] as $vote) {
                        // A vote can exist in (currently) one of four status codes.
                        // Active, old (a vote that flipped in the past), deleted (user deleted their account), or nonmember (votes fall off if banned or left of own volition).
                        // We're retrieving all the active ones for now, and will flip them if needed.
                        if($wikidotusers->contains($vote["user_id"]) == false) {
                            // No existing vote from this user, make a new row.
                            $v = new Vote([
                                'page_id' => $page->id,
                                'user_id' => auth()->id(),
                                'wd_user_id' => $vote["user_id"],
                                'wd_vote_ts' => Carbon::now(),
                                'JsonTimestamp' => Carbon::now()
                            ]);
                            if ($vote["vote"] == "+") {
                                $v->vote = 1;
                            } else if ($vote["vote"] == "-") {
                                $v->vote = -1;
                            } else {
                                $v->vote = $vote["vote"];
                            }

                            //It's possible a user has voted and then deleted their account, so their status is not yet determined.
                            if(strpos($vote["username"], "Deleted Account ") === 0) {
                                $v->metadata = json_encode(array('status' => 'deleted'));
                            }
                            else {
                                $v->metadata = json_encode(array('status' => 'active'));
                            }

                            $v->save();
                        }
                        else{
                            // We have a vote from this user on this article. This is normal as we're just checking on a
                            // schedule and generally speaking, votes won't change. So let's get some stuff out of the way quickly.
                            $oldvote = $votes->where('wd_user_id',$vote["user_id"])->first();
                            if($oldvote->status == 'deleted') {
                                // Deleted accounts aren't gonna change their vote, move on.
                                continue;
                            }

                            // Let's figure out if the vote we were sent is an upvote or a downvote,
                            if ($vote["vote"] == "+") {
                                $newvote = 1;
                            } else if ($vote["vote"] == "-") {
                                $newvote = -1;
                            } else {
                                $newvote = $vote["vote"];
                            }

                            if ($oldvote->vote == $newvote) {
                                // The vote didn't change, but the user could have still left.
                                if(strpos($vote["username"], "Deleted Account ") === 0) {
                                    $oldvote->metadata = json_encode(array('status' => 'deleted'));
                                    $oldvote->JsonTimestamp = Carbon::now();
                                    $oldvote->save();
                                }
                            }
                            else {
                                // The user has flipped their vote. Call the old one, well, old. Otherwise we'll get a
                                // unique index constraint on a triple of wd_user_id, page_id, and status.
                                $oldvote->metadata=json_encode(array('status' => 'old'));
                                $oldvote->JsonTimestamp = Carbon::now();
                                $oldvote->save();
                                // Save the new one.
                                $v = new Vote([
                                    'page_id' => $page->id,
                                    'user_id' => auth()->id(),
                                    'vote' => $newvote,
                                    'wd_user_id' => $vote["user_id"],
                                    'wd_vote_ts' => Carbon::now(),
                                    'JsonTimestamp' => Carbon::now()
                                ]);
                                // It's possible a user has voted and then deleted their account, so their status is not yet determined.
                                if(strpos($vote["username"], "Deleted Account ") === 0) {
                                    $v->metadata = json_encode(array('status' => 'deleted'));
                                }
                                else {
                                    $v->metadata = json_encode(array('status' => 'active'));
                                }
                                $v->save();
                            }
                        }
                    }

                    // Now, let's compare the old and new lists and see who removed their vote (essentially setting a no-vote).
                    $removedvoters = array_values(array_diff($oldvoters,$newvoters));
                    foreach($removedvoters as $rv) {
                        $oldvote = $votes->where('wd_user_id', $rv)->first();
                        $oldvote->JsonTimestamp = Carbon::now();
                        $newvote = $oldvote->replicate(['status']);

                        // Old one is old.
                        $oldvote->metadata = json_encode(array('status' => 'old'));
                        $oldvote->save();

                        // New one is 0.
                        $newvote->vote = 0;
                        $newvote->save();
                    }

                    if(isset($oldmetadata["page_missing_votes"])) {
                        unset($oldmetadata["page_missing_votes"]); // Cleanup in case this is the first request.
                        $page->metadata = json_encode($oldmetadata);
                        $page->jsonTimestamp = Carbon::now(); // touch on update
                        $page->save();
                    }
                    return response('saved');
            }
        }
    }

    public function get_pages_missing_metadata(Domain $domain)
    {
        $pages = DB::table('pages')
            ->select('slug')
            ->where('wiki_id',$domain->wiki->id)
            ->whereRaw('JSON_CONTAINS_PATH(metadata, "one", "$.page_missing_metadata")')
            ->pluck('slug');
        return response($pages->toJson());
    }

    public function put_page_thread_id(Domain $domain, Request $request)
    {
        if(Gate::allows('write-programmatically')) {
            $p = Page::where('wd_page_id', $request["wd_page_id"])->get();
            if($p->isEmpty()) {
                // Well this is awkward.
                // 2stacks just sent us metadata about a page we don't have.
                // Summon the troops.
                Log::error('2stacks sent us a thread id for ' . $request["wd_page_id"]. ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching page!');
                Log::error('$request: ' . $request);
                return response('I don\'t have a page to attach this data to!', 500)
                    ->header('Content-Type', 'text/plain');
            }
            else {
                $page = $p->first();
                $metadata = json_decode($page->metadata, true);
                if(isset($metadata["page_missing_comments"]) && $metadata["page_missing_comments"] == true) {
                    if(isset($metadata["wd_thread_id"])) {
                        // We already had the thread ID, let's requeue the job to extract comments.
                        $job = new PushThreadId($metadata["wd_thread_id"], $domain->wiki->id);
                        $job->send('scuttle-threads-missing-comments');
                        return response('had that one already');
                    }
                    else {
                        // This is our expected condition for having this method run.

                        // Attach the thread to page metadata.
                        $metadata["wd_thread_id"] = $request["wd_thread_id"];

                        // Stub out the thread with the wd_thread_id.
                        $thread = new Thread;
                        $thread->wd_thread_id = $request["wd_thread_id"];
                        $thread->user_id = auth()->id();
                        $thread->metadata = json_encode(array("thread_missing_posts" => true));
                        $thread->JsonTimestamp = Carbon::now();
                        $thread->save();

                        // Queue the job to get comments.
                        $job = new PushThreadId($metadata["wd_thread_id"], $domain->wiki->id);
                        $job->send('scuttle-threads-missing-comments');

                        // Save the changes and return.
                        $page->metadata = json_encode($metadata);
                        $page->JsonTimestamp = Carbon::now();
                        $page->save();
                        return response('saved', 200);
                    }
                }
            }


        }
    }

    public function put_page_files(Domain $domain, Request $request)
    {
        if (Gate::allows('write-programmatically')) {
            $page = Page::where('slug', $request["slug"])->orderBy('milestone', 'desc')->first();
            if($page == null) {
                // Well this is awkward.
                // 2stacks just sent us files for a page we don't have.
                // Summon the troops.
                Log::error('2stacks sent us a file for ' . $request["wd_page_id"]. ' for wiki ' . $domain->wiki->id . ' but SCUTTLE doesn\'t have a matching page!');
                Log::error('$request: ' . $request);
                return response('I don\'t have a page to attach this file to!', 500)
                    ->header('Content-Type', 'text/plain');
            }
            // We will have a lot of pages with no files to attach but they'll all let us know one way or another.
            // We could do this at any time and it was simpler to fire this once a page had metadata attached.
            if ($request["has_files"] == false) {
                $metadata = json_decode($page->metadata, true);
                unset($metadata["page_missing_files"]);
                $page->metadata = json_encode($metadata);
                $page->JsonTimestamp = Carbon::now();
                $page->save();
            } else {
                // 2stacks has sent us a link to a file and some metadata about it. We know there's only one file in the payload.
                $file = new File([
                    'page_id' => $page->id,
                    'filename' => $request["filename"],
                    'path' => $request["path"],
                    'size' => $request["size"],
                    'metadata' => json_encode($request["metadata"]),
                    'JsonTimestamp' => Carbon::now()
                ]);
                try {
                    $file->save();
                } catch (\PDOException $e) {
                    // We already had that one. Lambdas can stack up and this can happen through SQS multiballin'.
                }
            }
        }
    }

    public function delete_page(Domain $domain, Request $request, $id)
    {
        if(Gate::allows('write-programmatically')) {
            // 2stacks sent us a page ID that is no longer valid at Wikidot, that we sent to them. Time to say goodbye.

            // First, let's verify that the delete request matches a page that we actually noticed was missing in the
            // manifest and flagged accordingly.
            $page = Page::where('wiki_id', $domain->wiki_id)->where('wd_page_id', $id)->first();
            $metadata = json_decode($page->metadata, true);

            if(isset($metadata["page_missing"]) && $metadata["page_missing"] == true) {
                // Press F to pay respect
                unset($metadata["page_missing"]);
                $page->metadata = json_encode($metadata);
                $page->delete();

                discord(
                    'page-deleted',
                    "Deleting `".$page->slug."` (\"".$metadata["wikidot_metadata"]["title"]."\", SCUTTLE ID `".$page->id."`) after it was flagged missing and then not found at Wikidot."
                );
            }
            else {
                // Now this is concerning. We got an instruction to delete a page that came from outside the normal workflow.
                // Fire a notification to investigate.
                discord(
                    'security',
                    "<@350660518408880128> SCUTTLE received a request to delete page ".$metadata["wikidot_metadata"]["title"]." (SCUTTLE ID `".$page->id."`) but it is not flagged as missing.\nIP address: `".$request->ip()."`\nUser ID: ".auth()->id()
                );
            }
        }
    }
}
