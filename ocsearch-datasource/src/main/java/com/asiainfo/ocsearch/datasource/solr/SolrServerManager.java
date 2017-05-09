package com.asiainfo.ocsearch.datasource.solr;

import java.util.Properties;

/**
 * Created by mac on 2017/5/9.
 */
public class SolrServerManager {
    private static SolrServer instance;
    public synchronized  static void setUp(Properties prop){
        instance = new SolrServer(prop);
    }

    public static SolrServer getInstance() {
        return instance;
    }
}
