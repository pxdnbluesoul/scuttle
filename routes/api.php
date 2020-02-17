<?php

use Illuminate\Http\Request;
use App\Wiki;
use App\Domain;

/*
|--------------------------------------------------------------------------
| API Routes
|--------------------------------------------------------------------------
|
| Here is where you can register API routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| is assigned the "api" middleware group. Enjoy building your API!
|
*/
app('debugbar')->disable();

Route::middleware('auth:api')->get('/user', function (Request $request) {
    return $request->user();
});

Route::middleware('auth:api', 'throttle:10000,1')->group(function() {
    Route::domain('{domain}')->group(function() {
        Route::get('pages', 'API\PageController@index');
        Route::put('wikidot', 'API\PageController@wdstore');
        Route::get('/wikidot/metadata', 'API\PageController@getwikidotmetadata');
        Route::put('/wikidot/metadata', 'API\PageController@putwikidotmetadata');
        Route::get('revisions', 'API\PageController@revisions');
        Route::get('scrape/revisions/manifest', 'API\PageController@getscrapemanifest');
        Route::put('/scrape/revisions', 'API\PageController@putscraperevision');
        Route::put('/scrape/complete', 'API\PageController@recalculatediffs');
        Route::put('/pages/wikidotids', 'API\PageController@putwikidotids');
        Route::get('/pages/get/wikidotid', 'API\PageController@getwikidotids');
        Route::get('/pages/get/wikidotid/last', 'API\PageController@lastwikidotid');

        // 2stacks routes:
        Route::put('/2stacks/pages/manifest', 'API\PageController@put_2stacks_pages_manifest')->middleware('scope:write-metadata');
        Route::get('/pages/missing/metadata', 'API\PageController@get_pages_missing_metadata')->middleware('scope:read-metadata');
        Route::put('/2stacks/page/metadata', 'API\PageController@put_page_metadata')->middleware('scope:write-metadata');
        Route::put('/2stacks/page/revisions', 'API\RevisionController@put_page_revisions')->middleware('scope:write-revision');
        Route::put('/2stacks/revision/content', 'API\RevisionController@put_revision_content')->middleware('scope:write-revision');
        Route::put('/2stacks/user/metadata', 'API\WikidotUserController@put_wikidot_user_metadata')->middleware('scope:write-metadata');
        Route::put('/2stacks/page/thread', 'API\PageController@put_page_thread_id')->middleware('scope:write-metadata');
        Route::put('/2stacks/page/votes', 'API\PageController@put_page_votes')->middleware('scope:write-votes');
        Route::put('/2stacks/thread/posts', 'API\PostController@put_thread_posts')->middleware('scopes:write-post,write-thread');
        Route::put('/2stacks/page/files', 'API\PageController@put_page_files')->middleware('scope:write-file');
        Route::put('/2stacks/forum/metadata', 'API\ForumController@put_forum_metadata')->middleware('scopes:write-metadata');
        Route::put('/2stacks/scheduled/page/metadata', 'API\PageController@sched_pages_metadata')->middleware('scope:write-metadata');
        Route::put('/2stacks/forum/threads', 'API\ForumController@put_forum_threads')->middleware('scope:write-post');
        Route::delete('/2stacks/page/delete/{id}', 'API\PageController@delete_page')->middleware('scope:write-metadata');

        //API v1 routes:
        Route::prefix('v1')->group(function() {
           Route::get('page', 'API\v1\PageController@page_get_page')->middleware('scope:read-metadata');
           Route::get('page/{id}', 'API\v1\PageController@page_get_page_ID')->where(['id' => '[0-9]{1,10}'])->middleware('scope:read-article');
           Route::get('page/slug/{slug}', 'API\v1\PageController@page_get_page_slug_SLUG')->where(['slug' => '(^[a-z0-9][a-z0-9-]{0,59}[a-z0-9]$)|^[a-z0-9]{1}$'])->middleware('scope:read-article');
           Route::get('page/{id}/revisions', 'API\v1\PageController@page_get_page_ID_revisions')->where(['id' => '[0-9]{1,10}'])->middleware('scope:read-metadata');
           Route::post('page/revisions', 'API\v1\PageController@page_post_page_revisions')->where(['id' => '[0-9]{1,10}'])->middleware('scope:read-revision');
           Route::get('page/{id}/votes', 'API\v1\PageController@page_get_page_ID_votes')->where(['id' => '[0-9]{1,10}'])->middleware('scope:read-metadata');
        });
    });
});
