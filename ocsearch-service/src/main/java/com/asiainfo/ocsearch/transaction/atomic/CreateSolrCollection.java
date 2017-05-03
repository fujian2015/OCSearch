package com.asiainfo.ocsearch.transaction.atomic;

import com.asiainfo.ocsearch.datasource.solr.SolrServer;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by mac on 2017/3/30.
 */
public class CreateSolrCollection implements AtomicOperation, Serializable {

    private static Logger log = Logger.getLogger(CreateSolrCollection.class);

    private final String config;
    private final String collection;
    private final int shards;
    private final int solrReplicas;

    public CreateSolrCollection(String collection, String config, int solrShards, int solrReplicas) {
        this.collection = collection;
        this.config = config;
        this.shards = solrShards;
        this.solrReplicas = solrReplicas;
    }

    public boolean execute() {
        try {
            log.info("create solr collection " + collection + " start!");

            SolrServer.getInstance().createCollection(collection, config, shards);

            log.info("create solr collection " + collection + " success!");
        } catch (Exception e) {
            log.warn("create solr collection " + collection + " failure!", e);
            throw new RuntimeException("create solr collection failure!", e);
        }

        return true;
    }

    public boolean recovery() {
        try {
            log.info("delete solr collection " + collection + " start!");

            SolrServer solrServer = SolrServer.getInstance();
            if (solrServer.existCollection(collection)) {
                solrServer.deleteCollection(collection);
            }
            log.info("delete solr collection " + collection + " success!");
        } catch (Exception e) {
            log.warn("delete solr collection " + collection + " failure!", e);
            throw new RuntimeException("delete solr collection failure!", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        try {

            SolrServer solrServer = SolrServer.getInstance();

            boolean exist = solrServer.existCollection(collection);

            if (exist)
                log.warn("solr collcetion " + collection + " has existed");
            return !exist;
        } catch (Exception e) {
            log.error("check solr collection " + collection + " failure!", e);
            throw new RuntimeException("check solr collection failure!", e);
        }

    }
}
