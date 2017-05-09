package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.meta.SchemaManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;

/**
 * Created by mac on 2017/5/5.
 */
public class SaveTableToMemory implements AtomicOperation{

    String table;
    String schema;

    public SaveTableToMemory(String table, String schema) {
        this.schema = schema;
        this.table=table;
    }

    @Override
    public boolean execute() {

        return SchemaManager.addTable(table, schema);
    }

    @Override
    public boolean recovery() {
        return SchemaManager.removeTable(table);
    }

    @Override
    public boolean canExecute() {
        return true;
    }
}
