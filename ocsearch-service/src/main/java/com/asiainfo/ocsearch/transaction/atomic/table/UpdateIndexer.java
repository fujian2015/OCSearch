package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.meta.Schema;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/7/11.
 */
public class UpdateIndexer extends  UpdateOrAddIndexer{

    static Logger log = Logger.getLogger("state");

    public UpdateIndexer(String table, Schema tableSchema) {
        super(table, tableSchema);
    }

    @Override
    public boolean execute() {

        log.info("update indexer table " + table + " start!");
        try {
            IndexerServiceManager.getIndexerService().updateTable(table,getIndexerConf(table));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("update habse-indexer table " + table + " failure!", e);
        } finally {

        }
        log.info("update indexer table " + table + " success!");

        return true;
    }

    @Override
    public boolean recovery() {

        return true;
    }

    @Override
    public boolean canExecute() {
        return IndexerServiceManager.getIndexerService().exists(table);
    }


}
