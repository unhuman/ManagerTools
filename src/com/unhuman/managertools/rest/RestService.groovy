package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import groovy.json.JsonSlurper
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder
import org.apache.hc.core5.http.message.BasicClassicHttpRequest

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

class RestService {

//    private static enum ConnectionManager {
//        // Just one of me so constructor will be called once.
//        Client
//        // The pool
//        private PoolingHttpClientConnectionManager cm
//
//        // The constructor creates it - thus late
//        private ConnectionManager() {
//            cm = new PoolingHttpClientConnectionManager()
//            // Increase max total connection to 200
//            cm.setMaxTotal(2)
//            // Increase default max connection per route to 20
//            cm.setDefaultMaxPerRoute(20)
//        }
//
//        CloseableHttpClient get() {
//            CloseableHttpClient threadSafeClient = HttpClients.custom()
//                    .setConnectionManager(cm)
////                    .setDefaultRequestConfig(ConnectionManager.getRequestConfig())
//                    .build()
//            return threadSafeClient
//        }
//    }


    static Object GetRequest(String uri, String authCookies, NameValuePair... parameters) {
        BasicClassicHttpRequest request = ClassicRequestBuilder
                .create("GET")
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader("Cookie", authCookies)
                .setHeader(HttpHeaders.CONTENT_ENCODING, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .build();

        return executeRequest(request)
    }


    static Object GetRequest(String uri, String user, String password, NameValuePair... parameters) {
        BasicClassicHttpRequest request = ClassicRequestBuilder
                .create("GET")
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(HttpHeaders.AUTHORIZATION, getBasicAuth(user, password))
                .setHeader(HttpHeaders.CONTENT_ENCODING, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .build();

        return executeRequest(request)
    }

    static Object PutRequest(String uri, String authCookies, String content, NameValuePair... parameters) {
        BasicClassicHttpRequest request = ClassicRequestBuilder
                .create("PUT")
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader("Cookie", authCookies)
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .setEntity(new StringEntity(content))
                .build();

        return executeRequest(request)
    }


    private static Object executeRequest(BasicClassicHttpRequest request) {
        String responseData = HttpClientBuilder.create()
                .setDefaultRequestConfig(getRequestConfig())
                .build()
                .withCloseable { httpClient ->
                    httpClient.execute(request).withCloseable { response ->
                        if (response.getCode() < 200 || response.getCode() > 299) {
                            throw new RuntimeException("Error: Status ${response.getStatusLine().getStatusCode()}")
                        }
                        InputStream inputStream = response.getEntity().getContent()
                        String text = new String(inputStream.readAllBytes(), Charset.defaultCharset())
                        return text
                    }
        }

        return new JsonSlurper().parseText(responseData)

    }

    private static RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                .build()
    }

    private static String getBasicAuth(String user, String password) {
        return "Basic "+ Base64.encoder.encodeToString("${user}:${password}".getBytes(Charset.defaultCharset()));
    }
}
