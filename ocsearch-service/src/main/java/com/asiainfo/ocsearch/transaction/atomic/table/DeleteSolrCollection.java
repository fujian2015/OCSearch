package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.solr.SolrServer;
import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/5.
 */
public class DeleteSolrCollection implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String collection;
    public DeleteSolrCollection(String collection){
        this.collection =collection;
    }
    @Override
    public boolean execute() {
        try {
            log.info("delete solr collection " + collection + " start!");

            SolrServer solrServer = SolrServerManager.getInstance();
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
    public boolean recovery() {
        return true;
    }

    public boolean canExecute() {
       return true;
    }

}
