package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.SchemaManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;

/**
 * Created by mac on 2017/5/5.
 */
public class AddSchemaToMemory implements AtomicOperation {

    Schema schema;

    public AddSchemaToMemory(Schema schema) {
        this.schema = schema;
    }

    @Override
    public boolean execute() {

        SchemaManager.addSchema(schema.getName(), schema);
        return true;
    }

    @Override
    public boolean recovery() {
        return SchemaManager.removeSchema(schema.getName());
    }

    @Override
    public boolean canExecute() {
        return true;
    }
}
