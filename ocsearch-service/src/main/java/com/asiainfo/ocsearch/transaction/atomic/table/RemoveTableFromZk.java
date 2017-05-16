package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/14.
 */
public class RemoveTableFromZk implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String table;

    public RemoveTableFromZk(String table) {
        this.table = table;
    }

    @Override
    public boolean execute() {
        try {
            log.info("delete  table " + table + " from zookeeper start!");
            MetaDataHelperManager.getInstance().deleteTable(table);
            log.info("delete  table " + table + " from zookeeper success!");
        } catch (Exception e) {
            log.error("delete table " + table + " from zookeeper error!", e);
            throw new RuntimeException("delete table " + table + " from zookeeper error!", e);
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
