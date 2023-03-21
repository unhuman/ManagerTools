import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

class JiraREST {
    String jiraServer;
    String cookies;

    JiraREST(String jiraServer, String cookies) {
        this.jiraServer = jiraServer
        this.cookies = cookies
    }

    // Get Sprints
    // https://jira.x.com/rest/greenhopper/1.0/sprintquery/679?includeHistoricSprints=false&includeFutureSprints=false&_=1679081186798
    Object getSprints(String boardId) {
        String uri = "https://${jiraServer}/rest/greenhopper/1.0/sprintquery/${boardId}"
        NameValuePair rapidViewIdPair = new BasicNameValuePair("includeHistoricSprints", "false")
        NameValuePair sprintIdPair = new BasicNameValuePair("includeFutureSprints", "false")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(cookies, uri, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    // Get Sprint Report
    // https://jira.x.com/rest/greenhopper/1.0/rapid/charts/sprintreport?rapidViewId=679&sprintId=26636&_=1679081186799
    Object getSprintReport(String boardId, String sprintId) {
        String uri = "https://${jiraServer}/rest/greenhopper/1.0/rapid/charts/sprintreport"
        NameValuePair rapidViewIdPair = new BasicNameValuePair("rapidViewId", boardId)
        NameValuePair sprintIdPair = new BasicNameValuePair("sprintId", sprintId)
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(cookies, uri, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    //static Object getSprintReport(String user, String password, String boardId, String sprintId) {
    //    String uri = "https://${jiraServer}/rest/greenhopper/1.0/rapid/charts/sprintreport"
    //    NameValuePair rapidViewIdPair = new BasicNameValuePair("rapidViewId", boardId)
    //    NameValuePair sprintIdPair = new BasicNameValuePair("sprintId", sprintId)
    //    NameValuePair timeIdPair = new BasicNameValuePair("_", "1679335101555")
    //    return RestService.GetRequest(user, password, uri, rapidViewIdPair, sprintIdPair, timeIdPair)
    //}

    // get ticket info
    // https://jira.x.com/rest/api/latest/issue/CMOBL-146079
    Object getTicket(String ticketId) {
        String uri = "https://${jiraServer}/rest/api/latest/issue/${ticketId}"
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(cookies, uri, timeIdPair)
    }

    // Get pull request data
    // https://jira.x.com/rest/dev-status/1.0/issue/detail?issueId=3699559&applicationType=stash&dataType=pullrequest&_=1679440827558
    Object getTicketPullRequestInfo(String issueId) {
        String uri = "https://${jiraServer}/rest/dev-status/1.0/issue/detail"
        NameValuePair issueIdPair = new BasicNameValuePair("issueId", issueId)
        NameValuePair applicationTypePair = new BasicNameValuePair("applicationType", "stash")
        NameValuePair dataTypePair = new BasicNameValuePair("dataType", "pullrequest")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(cookies, uri, issueIdPair, applicationTypePair, dataTypePair, timeIdPair)
    }
}
