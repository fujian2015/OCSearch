package com.asiainfo.ocsearch.cache;

/**
 * Created by mac on 2017/5/19.
 */
public class CacheManager {

    static ICache cache = null;

    public static synchronized ICache getCache() {

        if (cache == null) {
            cache = new HbaseCache("cache", 300);
        }

        return cache;
    }
}
