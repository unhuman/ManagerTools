package com.unhuman.managertools.util

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

class ConfigFileManager {
    String filename
    def state

    ConfigFileManager(String filename) {
        filename = "${System.getProperty("user.home")}/${filename}"
        this.filename = filename
        try {
            state = new JsonSlurper().parseText(new File(this.filename).text)
        } catch (FileNotFoundException fnfe) {
            // default state
            state = [:]
        }
    }

    Boolean containsKey(String key) {
        // split the string key by periods and then walk the map
        // to see if the key exists
        def keys = key.split("\\.")
        def current = state
        for (String k : keys) {
            def matchedKey = current.keySet().find { it.toLowerCase().equals(k.toLowerCase()) }
            if (matchedKey == null) {
                return false
            }
            current = current[k]
        }

        return true
    }

    Object getValue(String key) {
        // split the string key by periods and then walk the map
        // to see if the key exists
        def keys = key.split("\\.", 2)
        def current = state
        for (String k : keys) {
            def matchedKey = current.keySet().find { it.toLowerCase().equals(k.toLowerCase()) }
            if (matchedKey == null) {
                throw new RuntimeException("Could not find key: ${key}")
            }
            current = current[matchedKey]
        }
        return current
    }

    void updateValue(String key, Object value) {
        // split the string key by periods and then walk the map
        // setting the last value as provided
        def keys = key.split("\\.")
        def current = state
        for (int i = 0; i < keys.size(); i++) {
            def subkey = keys[i]
            def matchedKey = current.keySet().find { it.toLowerCase().equals(subkey.toLowerCase()) }
            if (i == keys.size() - 1) {
                // write existing key if found, else write as provided
                current[(matchedKey != null) ? matchedKey : subkey] = value
            } else {
                if (matchedKey == null) {
                    throw new RuntimeException("Could not find key: ${key}")
                }
                current = current[matchedKey]
            }
        }

        File file = new File(filename)
        file.delete()
        file.createNewFile()
        file.write(new JsonBuilder(state).toPrettyString())
    }
}
