package com.unhuman.managertools.rest

import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.message.BasicHeader

import java.nio.charset.Charset

class AuthInfo {
    private String username
    private String password
    private String cookies

    AuthInfo(String username, String password) {
        this.username = username
        this.password = password
    }

    AuthInfo(String cookies) {
        this.cookies = cookies
    }

    String getUsername() {
        return username
    }

    String getPassword() {
        return password
    }

    String getCookies() {
        return cookies
    }

    Header getAuthHeader() {
        if (username != null && password != null) {
            return new BasicHeader("Authorization", getBasicAuth())
        }
        if (cookies != null) {
            return new BasicHeader("Cookie", cookies)
        }
        throw new RuntimeException("Invalid AuthInfo")
    }

    private String getBasicAuth() {
        return "Basic " + Base64.encoder.encodeToString("${username}:${password}".getBytes(Charset.defaultCharset()));
    }

}
