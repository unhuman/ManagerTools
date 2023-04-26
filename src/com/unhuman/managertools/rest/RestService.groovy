package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import groovy.json.JsonSlurper
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder
import org.apache.hc.core5.http.message.BasicClassicHttpRequest

import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class RestService {

    static Map<String, CloseableHttpClient> clients = new ConcurrentHashMap<>()

    /**
     * Simple connection manager - re-uses connections
     * @param authority
     * @return
     */
    private static CloseableHttpClient getClient(String authority) {
        synchronized (clients) {
            if (!clients.containsKey(authority)) {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                        .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                        .build()

                CloseableHttpClient client = HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .build()

                clients.put(authority, client)
            }

            return clients.get(authority)
        }
    }


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
        String responseData = getClient(request.getAuthority().getHostName())
                .with { httpClient ->
                    httpClient.execute(request).withCloseable { response ->
                        if (response.getCode() < 200 || response.getCode() > 299) {
                            throw new RuntimeException("Error: Status ${response.getCode()}")
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
