package com.unhuman.managertools.rest

import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

class BitbucketREST {
    String bitbucketServer;
    String cookies;

    BitbucketREST(String bitbucketServer, String cookies) {
        this.bitbucketServer = bitbucketServer
        this.cookies = cookies
    }

    // Get activies (approvals, comments, etc)
    // https://stash.x.net/rest/api/latest/projects/PT/repos/web-survey-hub/pull-requests/78/activities?avatarSize=48&start=0&limit=25&markup=true
    Object getActivities(String prUrl) {
        String uri = "${prUrl}/activities"
        NameValuePair startPair = new BasicNameValuePair("start", "0")
        NameValuePair limitPair = new BasicNameValuePair("limit", "100")
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")
        return RestService.GetRequest(uri, cookies, startPair, limitPair, markupPair)
    }
}
