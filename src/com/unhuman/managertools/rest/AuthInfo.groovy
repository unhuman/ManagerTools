package com.unhuman.managertools.rest

import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.message.BasicHeader

import java.nio.charset.Charset
import java.util.regex.Matcher
import java.util.regex.Pattern

class AuthInfo {
    private static final Pattern EXTRACT_COOKIE_INFO = Pattern.compile("([^=]*)=([^;]*).*")
    enum AuthType { Basic, Cookies, Bearer }
    private AuthType authType
    private String authentication

    AuthInfo(String username, String password) {
        this.authType = AuthType.Basic
        this.authentication = getBasicAuth(username, password)
    }

    AuthInfo(AuthType authType, String cookiesOrToken) {
        if (![AuthType.Cookies, AuthType.Bearer].contains(authType)) {
            throw new RuntimeException("Invalid AuthType provided: ${authType}")
        }
        this.authType = authType
        this.authentication = (authType == AuthType.Bearer) ? getBearer(cookiesOrToken) : cookiesOrToken
    }

    void updateCookies(List<BasicHeader> cookies) {
        if (AuthType.Cookies == authType) {
            // Replace cookies in memory
            for (int i = 0; i < cookies.size(); i++) {
                BasicHeader cookie = cookies.get(i)
                Matcher matcher = EXTRACT_COOKIE_INFO.matcher(cookie.getValue())
                if (matcher.matches()) {
                    String cookieName = matcher.group(1)
                    String cookieValue = matcher.group(2)
                    Pattern replaceFinder = Pattern.compile("${cookieName}=([^;]*)")
                    authentication = authentication.replaceFirst(replaceFinder, "${cookieName}=${cookieValue}")
                }
            }
        }
    }

    Header getAuthHeader() {
        switch (authType) {
            case AuthType.Basic:
            case AuthType.Bearer:
                return new BasicHeader("Authorization", authentication)
            case AuthType.Cookies:
                return new BasicHeader("Cookie", authentication)
            default:
                throw new RuntimeException("Invalid AuthInfo")
        }
    }

    private static String getBasicAuth(String username, String password) {
        return "Basic ${Base64.encoder.encodeToString("${username}:${password}".getBytes(Charset.defaultCharset()))}"
    }

    private static String getBearer(String token) {
        return "Bearer ${token}"
    }

}
