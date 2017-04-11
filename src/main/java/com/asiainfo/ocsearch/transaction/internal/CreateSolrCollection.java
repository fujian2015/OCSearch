package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.db.solr.SolrServer;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by mac on 2017/3/30.
 */
public class CreateSolrCollection implements AtomicOperation, Serializable {


    private final String config;
    private final String collection;
    private final int shards;

    Logger log= Logger.getLogger(getClass());

    public CreateSolrCollection(String collection, String config, int shards) {
        this.collection = collection;
        this.config = config;
        this.shards = shards;

    }

    public boolean execute() {
        try {
            log.info("create solr collection "+collection+" start!");

            SolrServer.getInstance().createCollection(collection, config, shards);

            log.info("create solr collection "+collection+" success!");
        } catch (Exception e) {
            log.warn("create solr collection "+collection+" failure!",e);
            throw new RuntimeException("create solr collection failure!", e);
        }

        return true;
    }

    public boolean recovery() {
        try {
            log.info("delete solr collection "+collection+" start!");

            SolrServer solrServer =  SolrServer.getInstance();
            if(solrServer.existCollection(collection)){
                solrServer.deleteCollection(collection);
            }
            log.info("delete solr collection "+collection+" success!");
        } catch (Exception e) {
            log.warn("delete solr collection "+collection+" failure!",e);
            throw new RuntimeException("delete solr collection failure!", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        try {
            SolrServer solrServer =  SolrServer.getInstance();

            return !solrServer.existCollection(collection);

        } catch (Exception e) {
            log.warn("check solr collection "+collection+" failure!",e);
            throw new RuntimeException("check solr collection failure!", e);
        }

    }
}
