package com.unhuman.managertools.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

class SprintDataCache {
    private static final String CACHE_DIR = "cacheData"
    private static final String CACHE_VERSION = "1.0"

    private static String sanitize(String s) {
        return (s ?: '').toLowerCase().replaceAll('[^a-z0-9]', '')
    }

    /**
     * Generate a human-readable cache key based on sprint parameters.
     * Non-alphanumeric characters (including date separators) are stripped so
     * the key is consistent regardless of formatting variations.
     */
    static String generateCacheKey(String teamName, String sprintName, String startDate, String endDate) {
        List<String> prefix = [sanitize(teamName), sanitize(sprintName)].findAll { it }
        String dateRange = "${sanitize(startDate)}-${sanitize(endDate)}"
        return (prefix + [dateRange]).join('_')
    }

    /**
     * Get the cache file path for a given cache key
     */
    static String getCacheFilePath(String cacheKey) {
        return "${CACHE_DIR}/${cacheKey}.json"
    }

    /**
     * Ensure the cache directory exists
     */
    static void ensureCacheDirectoryExists() {
        File cacheDir = new File(CACHE_DIR)
        if (!cacheDir.exists()) {
            cacheDir.mkdirs()
        }
    }

    /**
     * Check if cached data exists for the given parameters
     */
    static boolean hasCachedData(String teamName, String sprintName, String startDate, String endDate) {
        String cacheKey = generateCacheKey(teamName, sprintName, startDate, endDate)
        String filePath = getCacheFilePath(cacheKey)
        File cacheFile = new File(filePath)

        if (!cacheFile.exists()) {
            return false
        }

        // Check if the cache version matches
        try {
            def cachedData = new JsonSlurper().parse(cacheFile)
            return cachedData.version == CACHE_VERSION
        } catch (Exception e) {
            System.err.println("Error reading cache file ${filePath}: ${e.message}")
            return false
        }
    }

    /**
     * Load cached data
     */
    static Map loadCachedData(String teamName, String sprintName, String startDate, String endDate) {
        String cacheKey = generateCacheKey(teamName, sprintName, startDate, endDate)
        String filePath = getCacheFilePath(cacheKey)

        System.out.println("Loading cached data from: ${filePath}")

        File cacheFile = new File(filePath)
        def cachedData = new JsonSlurper().parse(cacheFile)

        if (cachedData.version != CACHE_VERSION) {
            throw new RuntimeException("Cache version mismatch. Expected ${CACHE_VERSION}, found ${cachedData.version}")
        }

        return cachedData.data as Map
    }

    /**
     * Save data to cache
     */
    static void saveToCache(String teamName, String sprintName, String startDate, String endDate, Map data) {
        ensureCacheDirectoryExists()

        String cacheKey = generateCacheKey(teamName, sprintName, startDate, endDate)
        String filePath = getCacheFilePath(cacheKey)

        def cacheData = [
            version: CACHE_VERSION,
            teamName: teamName,
            sprintName: sprintName,
            startDate: startDate,
            endDate: endDate,
            timestamp: System.currentTimeMillis(),
            data: data
        ]

        File cacheFile = new File(filePath)
        cacheFile.text = JsonOutput.prettyPrint(JsonOutput.toJson(cacheData))

        System.out.println("Saved data to cache: ${filePath}")
    }
}

