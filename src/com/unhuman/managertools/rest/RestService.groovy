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

    static Map<String, CloseableHttpClient> clients = new ConcurrentHashMap<>()

    RestService(AuthInfo authInfo) {
        this.authInfo = authInfo
    }

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
        String responseData = getClient(request.getAuthority().getHostName())
                .with { httpClient ->
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

    }

    private static RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                .build()
    }
}
