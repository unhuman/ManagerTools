import groovy.json.JsonSlurper
import org.apache.http.HttpHeaders
import org.apache.http.NameValuePair
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

import java.nio.charset.Charset

class RestService {
    static Object GetRequest(String authCookies, String uri, NameValuePair... parameters) {
        HttpUriRequest request = RequestBuilder
                .create("GET")
                .setConfig(getRequestConfig())
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader("Cookie", authCookies)
                .setHeader(HttpHeaders.CONTENT_ENCODING, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .build();

        return executeRequest(request)
    }


    static Object GetRequest(String user, String password, String uri, NameValuePair... parameters) {
        HttpUriRequest request = RequestBuilder
                .create("GET")
                .setConfig(getRequestConfig())
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(HttpHeaders.AUTHORIZATION, getBasicAuth(user, password))
                .setHeader(HttpHeaders.CONTENT_ENCODING, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .build();

        return executeRequest(request)
    }

    private static Object executeRequest(HttpUriRequest request) {
        String responseData = HttpClientBuilder.create().build().withCloseable { httpClient ->
            httpClient.execute(request).withCloseable { response ->

                if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() > 299) {
                    throw new RuntimeException("Error: Status ${response.getStatusLine().getStatusCode()}")
                }
                return EntityUtils.toString(response.getEntity());
            }
        }

        return new JsonSlurper().parseText(responseData)
    }

    private static RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(2000)
                .setSocketTimeout(3000)
                .build()
    };

    private static String getBasicAuth(String user, String password) {
        return "Basic "+ Base64.encoder.encodeToString("${user}:${password}".getBytes(Charset.defaultCharset()));
    }
}
