package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by mac on 2017/5/9.
 */
public class DeleteSolrConfig implements AtomicOperation {

    static Logger log = Logger.getLogger("state");
    String schema;

    public DeleteSolrConfig(String schema) {
        this.schema = schema;
    }

    @Override
    public boolean execute() {

        log.info("delete  solr config " + schema + " start!");

        try {
            if (SolrServerManager.getInstance().existConfig(schema))
                SolrServerManager.getInstance().deleteConfig(schema);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException("delete config " + schema + " failure!", e);
        }

        log.info("delete  solr config " + schema + " success!");

        return true;
    }


    @Override
    public boolean recovery() {
        return true;
    }

    @Override
    public boolean canExecute() {
        return true;
    }
}
