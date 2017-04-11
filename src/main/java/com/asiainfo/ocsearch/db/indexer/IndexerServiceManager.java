package com.asiainfo.ocsearch.db.indexer;

import com.asiainfo.ocsearch.db.solr.SolrServer;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;

import java.util.Properties;

/**
 * Created by mac on 2017/4/6.
 */
public class IndexerServiceManager {

    final private static String solrConfig = SolrServer.configFile;
    final private static String indexerConfig = "hbase-indexer.properties";
    private static IndexerService instance = null;

    public static  IndexerService getIndexerService() {

        if (instance == null) {

            Properties p = PropertiesLoadUtil.loadProFile(indexerConfig);

            String solrZk = PropertiesLoadUtil.loadProFile(solrConfig).getProperty("zookeeper", null);

            instance = new IndexerService(p.getProperty("indexer_home"), p.getProperty("zookeeper"), solrZk);
        }
        return instance;
    }
}
