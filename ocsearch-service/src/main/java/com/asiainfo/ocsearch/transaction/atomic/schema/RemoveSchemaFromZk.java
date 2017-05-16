package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/14.
 */
public class RemoveSchemaFromZk implements AtomicOperation{

    static Logger log = Logger.getLogger("state");

    String schema;

    public RemoveSchemaFromZk(String schema) {
        this.schema = schema;
    }

    @Override
    public boolean execute() {
        try {
            log.info("delete  schema " + schema + " from zookeeper start!");
            MetaDataHelperManager.getInstance().deleteSchema(schema);
            log.info("delete  schema " + schema + " from zookeeper success!");
        } catch (Exception e) {
            log.error("delete schema " + schema + " from zookeeper error!", e);
            throw new RuntimeException("delete schema " + schema + " from zookeeper error!", e);
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
