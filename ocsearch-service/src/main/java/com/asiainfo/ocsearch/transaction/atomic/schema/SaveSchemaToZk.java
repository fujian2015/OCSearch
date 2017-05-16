package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/14.
 */
public class SaveSchemaToZk implements AtomicOperation{
    static Logger log = Logger.getLogger("state");

    Schema schema;
    String schmaName;

    public SaveSchemaToZk( Schema schema) {

        this.schema = schema;
        this.schmaName=schema.getName();
    }

    @Override
    public boolean execute() {

        try {
            log.info("save  schema " + schmaName + " to zookeeper start!");
            MetaDataHelperManager.getInstance().createSchema(schema);
            log.info("save  schema " + schmaName+ " to zookeeper success!");
        } catch (Exception e) {
            log.error("save schema "+schmaName+" to zookeeper error!", e);
            throw new RuntimeException("save schema "+schmaName+" to zookeeper error!", e);
        }
        return true;
    }

    @Override
    public boolean recovery() {
        try {
            log.info("delete  schema " + schmaName + " from zookeeper start!");
            MetaDataHelperManager.getInstance().deleteSchema(schmaName);
            log.info("delete  schema " + schmaName + " from zookeeper success!");
        } catch (Exception e) {
            log.error("delete schema "+schmaName+" from zookeeper error!", e);
            throw new RuntimeException("delete schema "+schmaName+" from zookeeper error!", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        if (MetaDataHelperManager.getInstance().hasSchema(schmaName)) {
            return false;
        }
        return true;
    }
}
