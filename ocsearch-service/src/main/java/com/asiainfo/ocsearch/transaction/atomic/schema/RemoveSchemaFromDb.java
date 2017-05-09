package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Created by mac on 2017/5/9.
 */
public class RemoveSchemaFromDb implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String schema;

    public RemoveSchemaFromDb(String schema) {
        this.schema = schema;
    }

    @Override
    public boolean execute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String deleteSchema = "delete  from  schema_def where `name` = '" + schema + "'";
            String deleteField = "delete from  field_def where `schema_name` = '" + schema + "'";
            log.info(deleteField + " start!");
            myBaseService.delete(deleteField);
            log.info(deleteField + " success!");
            log.info(deleteSchema + " start!");
            myBaseService.delete(deleteSchema);
            log.info(deleteSchema + " success!");
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("delete schema " + schema + " from db failure", e);
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
