import groovy.json.JsonSlurper
import org.apache.http.HttpHeaders
import org.apache.http.NameValuePair
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

class RestService {
    static Object GetRequest(String uri, String authCookies, NameValuePair... parameters) {
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


    static Object GetRequest(String uri, String user, String password, NameValuePair... parameters) {
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

    static Object PutRequest(String uri, String authCookies, String content, NameValuePair... parameters) {
        HttpUriRequest request = RequestBuilder
                .create("PUT")
                .setConfig(getRequestConfig())
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader("Cookie", authCookies)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .setEntity(new StringEntity(content))
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
                .setSocketTimeout(30000)
                .build()
    };

    private static String getBasicAuth(String user, String password) {
        return "Basic "+ Base64.encoder.encodeToString("${user}:${password}".getBytes(Charset.defaultCharset()));
    }
}
