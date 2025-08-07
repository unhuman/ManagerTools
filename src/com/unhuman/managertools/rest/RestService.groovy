package com.unhuman.managertools.rest

import com.unhuman.managertools.rest.exceptions.NeedsRetryException
import com.unhuman.managertools.rest.exceptions.RESTException
@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import groovy.json.JsonSlurper
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder
import org.apache.hc.core5.http.message.BasicHeader

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
        ClassicRequestBuilder requestBuilder = createGithubRequestBuilder("GET", uri, parameters)
        ClassicHttpRequest request = requestBuilder.build()
        return executeRequest(request)
    }


    Object putRequest(String uri, String content, NameValuePair... parameters) {
        ClassicRequestBuilder requestBuilder = createGithubRequestBuilder("PUT", uri, parameters)

        requestBuilder.setEntity(new StringEntity(content))

        ClassicHttpRequest request = requestBuilder.build()

        return executeRequest(request)
    }


    private ClassicRequestBuilder createGithubRequestBuilder(String method, String uri,
                                                        NameValuePair... parameters) {
        return ClassicRequestBuilder
                .create(method)
                .setUri(uri)
                .setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8")
                .setHeader(authInfo.getAuthHeader())
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8")
                .addParameters(parameters)
    }

    private Object executeRequest(ClassicHttpRequest request) {
        AuthInfo useAuthInfo = authInfo

        CloseableHttpClient client = null

        do {
            try {
                client = getClient(request.getAuthority().getHostName())

                String responseData = client.with { httpClient ->
                    httpClient.execute(request).withCloseable { response ->
                        // 429 = rate limit exceeded, 403 = forbidden (but could be rate limit as well)
                        if ((response.getCode() == 429) || (response.getCode() == 403)) {
                            // Log Rate Limit Response Headers
                            def rateLimitHeaders = response.getHeaders().findAll { it.getName().containsIgnoreCase("RateLimit") }
                            if (rateLimitHeaders.size() > 0) {
                                System.err.println("Rate Limit Response Headers: ${rateLimitHeaders}")
                            }

                            // Check for Retry-After header
                            Integer retryAfter = response.getFirstHeader("Retry-After")?.getValue()?.toInteger()
                            if (retryAfter != null) {
                                InputStream inputStream = response.getEntity().getContent()
                                String responseContent = new String(inputStream.readAllBytes(), Charset.defaultCharset())
                                throw new NeedsRetryException(response.getCode(), responseContent, request.getUri().toString(), retryAfter)
                            }

                            // Check for X-RateLimit-Remaining and X-RateLimit-Reset headers
                            Integer rateLimitRemaining = response.getFirstHeader("X-RateLimit-Remaining")?.getValue()?.toInteger()
                            Long rateLimitReset = response.getFirstHeader("X-RateLimit-Reset")?.getValue()?.toLong()
                            if (rateLimitRemaining == 0 && rateLimitReset != null) {
                                retryAfter = (Integer) (rateLimitReset - (System.currentTimeMillis() / 1000))
                                if (retryAfter > 0) {
                                    InputStream inputStream = response.getEntity().getContent()
                                    String responseContent = new String(inputStream.readAllBytes(), Charset.defaultCharset())
                                    throw new NeedsRetryException(response.getCode(), responseContent, request.getUri().toString(), retryAfter)
                                }
                            }
                            throw new RESTException(response.getCode(), "Forbidden - no Rate Limit headers found: ${response.getHeaders()}", request.getUri().toString())
                        }

                        if (response.getCode() < 200 || response.getCode() > 299) {
                            throw new RESTException(response.getCode(), "Unable to retrieve requested url " + response.getReasonPhrase(), request.getUri().toString())
                        }

                        // Successful response, update cookies if available
                        List<BasicHeader> cookies = response.getHeaders("Set-Cookie").toList().collect { Header header ->
                            new BasicHeader("Cookie", header.getValue().split(";")[0])
                        }
                        useAuthInfo.updateCookies(cookies)
                        InputStream inputStream = response.getEntity().getContent()
                        String text = new String(inputStream.readAllBytes(), Charset.defaultCharset())
                        return text
                    }
                }
                return new JsonSlurper().parseText(responseData)
            } catch (NeedsRetryException nre) {
                System.err.println("Rate limit exceeded. Details: ${nre.toString()}")
                Thread.sleep(nre.getRetryAfter() * 1000)
            } catch (SocketTimeoutException ste) {
                System.err.println("Timeout exceeded. Details: ${ste.toString()}")
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
        } while (true)
    }

    private static RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout((long) 2L, TimeUnit.SECONDS)
                .setResponseTimeout((long) 60L, TimeUnit.SECONDS)
                .build()
    }
}
