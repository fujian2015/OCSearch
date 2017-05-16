package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.meta.Table;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/14.
 */
public class SaveTableToZk implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    Table table;
    String tableName;

    public SaveTableToZk(Table table) {

        this.table = table;
        this.tableName=table.getName();
    }

    @Override
    public boolean execute() {

        try {
            log.info("save  table " + tableName + " to zookeeper start!");
            MetaDataHelperManager.getInstance().createTable(table);
            log.info("save  table " + tableName + " to zookeeper success!");
        } catch (Exception e) {
            log.error("save table "+tableName+" to zookeeper error!", e);
            throw new RuntimeException("save table "+tableName+" to zookeeper error!", e);
        }
        return true;
    }

    @Override
    public boolean recovery() {
        try {
            log.info("delete  table " + tableName + " from zookeeper start!");
            MetaDataHelperManager.getInstance().deleteTable(tableName);
            log.info("delete  table " + tableName + " from zookeeper success!");
        } catch (Exception e) {
            log.error("delete table "+tableName+" from zookeeper error!", e);
            throw new RuntimeException("delete table "+tableName+" from zookeeper error!", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        if (MetaDataHelperManager.getInstance().hasTable(tableName)) {
            return false;
        }
        return true;
    }
}
