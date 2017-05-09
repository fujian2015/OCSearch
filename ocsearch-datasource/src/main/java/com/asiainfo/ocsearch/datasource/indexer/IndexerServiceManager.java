package com.asiainfo.ocsearch.datasource.indexer;

import java.util.Properties;

/**
 * Created by mac on 2017/4/6.
 */
public class IndexerServiceManager {

    private static IndexerService instance = null;

    public static IndexerService getIndexerService() {

        return instance;
    }

    public synchronized  static void setUp(Properties p) throws Exception {
        try {
            instance = new IndexerService(p.getProperty("solr.zookeeper"));
        } catch (Exception e) {
            throw e;
        }
    }
}
