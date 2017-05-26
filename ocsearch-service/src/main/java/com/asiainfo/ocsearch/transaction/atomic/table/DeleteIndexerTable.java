package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.indexer.IndexerService;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/5.
 */
public class DeleteIndexerTable implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    final String table;

    public DeleteIndexerTable(String table) {
        this.table = table;
    }
    @Override
    public boolean execute() {

        IndexerService indexerService = IndexerServiceManager.getIndexerService();
        try {
            log.info("delete indexer table " + table + " start!");
            if (indexerService.exists(table))
                indexerService.deleteTable(table);
            log.info("delete indexer table " + table + " success!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
