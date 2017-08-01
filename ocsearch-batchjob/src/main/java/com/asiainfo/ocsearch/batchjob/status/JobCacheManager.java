package com.asiainfo.ocsearch.batchjob.status;

import com.asiainfo.ocsearch.cache.ICache;

public class JobCacheManager {
    static ICache cache = null;

    public static synchronized ICache getCache() {

        if (cache == null) {
            cache = new JobStatusCache("jobstatus", 300);
        }

        return cache;
    }
}
