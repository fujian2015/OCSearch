package com.asiainfo.ocsearch.datasource.indexer;

import java.util.Properties;

/**
 * Created by mac on 2017/4/6.
 */
public class IndexerServiceManager {

    private static IndexerService instance = null;

    public static  IndexerService getIndexerService() {

        return instance;
    }

    public synchronized  static void setUp(Properties p){
        instance = new IndexerService(p.getProperty("indexer_home"), p.getProperty("zookeeper"), p.getProperty("solr_zk"));
    }
}
