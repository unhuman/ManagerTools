package com.unhuman.managertools.rest

import com.unhuman.managertools.rest.exceptions.RESTException
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

abstract class RestService {
    private final AuthInfo authInfo

    static Map<String, List<CloseableHttpClient>> clients = new ConcurrentHashMap<>()

    RestService(AuthInfo authInfo) {
        this.authInfo = authInfo
    }

    /**
     * Simple connection manager - re-uses connections
     * @param authority
     * @return
     */
    private static synchronized CloseableHttpClient getClient(String authority) {
        // this doesn't seem to work well in multi-threaded usages
        if (!clients.containsKey(authority) || clients.get(authority).isEmpty()) {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                    .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                    .build()

            CloseableHttpClient client = HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .build()

            return client
        }

        // return the first client we find
        return clients.get(authority).remove(0)
    }

    private static synchronized void returnClient(String authority, CloseableHttpClient client) {
        if (clients.get(authority) == null) {
            clients.put(authority, new ArrayList<>())
        }
        clients.get(authority).add(client)
    }

    Object getRequest(String uri, NameValuePair... parameters) {
        BasicClassicHttpRequest request = ClassicRequestBuilder
                .create("GET")
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(authInfo.getAuthHeader())
                .setHeader(HttpHeaders.CONTENT_ENCODING, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .build();

        return executeRequest(request)
    }


    Object putRequest(String uri, String content, NameValuePair... parameters) {
        BasicClassicHttpRequest request = ClassicRequestBuilder
                .create("PUT")
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(authInfo.getAuthHeader())
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8")
                .addParameters(parameters)
                .setEntity(new StringEntity(content))
                .build()

        return executeRequest(request)
    }


    private Object executeRequest(BasicClassicHttpRequest request) {
        AuthInfo useAuthInfo = authInfo

        CloseableHttpClient client = null
        try {
            client = getClient(request.getAuthority().getHostName())

            String responseData = client.with { httpClient ->
                httpClient.execute(request).withCloseable { response ->
                    if (response.getCode() < 200 || response.getCode() > 299) {
                        throw new RESTException(response.getCode(), "Unable to retrieve requested url", request.getUri().toString())
                    }
                    useAuthInfo.updateCookies(response.getHeaders("Set-Cookie").toList())
                    InputStream inputStream = response.getEntity().getContent()
                    String text = new String(inputStream.readAllBytes(), Charset.defaultCharset())
                    return text
                }
            }
            return new JsonSlurper().parseText(responseData)
        } catch (Exception e) {
            System.err.println("Request Error: ${e.getMessage()}")
            client.close()
            client = null
            throw e
        } finally {
            if (client != null) {
                returnClient(request.getAuthority().getHostName(), client)
            }
        }
    }

    private static RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                .build()
    }
}
