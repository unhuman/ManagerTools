package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

class BitbucketREST {
    String bitbucketServer
    AuthInfo authInfo

    BitbucketREST(String bitbucketServer, String username, String password) {
        this.bitbucketServer = bitbucketServer

        // convert user / password to cookies
        this.authInfo = new AuthInfo(username, password)

//        String url = "${bitbucketServer}/site/oauth2/access_token"
//        // -d grant_type=authorization_code
    }

    BitbucketREST(String bitbucketServer, String cookies) {
        this.bitbucketServer = bitbucketServer


        this.authInfo = new AuthInfo(cookies)
    }

    // Get activies (approvals, comments, etc)
    // https://stash.x.net/rest/api/latest/projects/PT/repos/web-survey-hub/pull-requests/78/activities?avatarSize=48&start=0&limit=25&markup=true
    Object getActivities(String prUrl) {
        String uri = "${prUrl}/activities"
        NameValuePair startPair = new BasicNameValuePair("start", "0")
        NameValuePair limitPair = new BasicNameValuePair("limit", "100")
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")
        return RestService.GetRequest(uri, authInfo, startPair, limitPair, markupPair)
    }
}
