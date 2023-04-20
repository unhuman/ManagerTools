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
        return state.containsKey(key)
    }

    String getValue(String key) {
        return state[key]
    }

    void updateValue(String key, String value) {
        state[key] = value
        File file = new File(filename)
        file.delete()
        file.createNewFile()
        file.write(new JsonBuilder(state).toPrettyString())
    }
}
