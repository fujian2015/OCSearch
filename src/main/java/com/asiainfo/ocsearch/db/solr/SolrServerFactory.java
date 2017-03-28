package com.asiainfo.ocsearch.db.solr;

/**
 * Created by mac on 2017/3/22.
 */
public class SolrServerFactory {

    private static final String configFile = "solr.properties";

    private static SolrConfig solrConfig = null;

    public static SolrServer createSolrServer() {
        if (solrConfig == null)
            solrConfig = loadSolrConfig(configFile);
        return new SolrServer(solrConfig);
    }

    private static SolrConfig loadSolrConfig(String configFile) {
        return new SolrConfig(configFile);
    }


}
