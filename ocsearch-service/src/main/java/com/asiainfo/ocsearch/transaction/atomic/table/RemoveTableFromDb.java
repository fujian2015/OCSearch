package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Created by mac on 2017/5/5.
 */
public class RemoveTableFromDb implements AtomicOperation{

    static Logger log = Logger.getLogger("state");

    String table;

    public RemoveTableFromDb(String table) {
        this.table = table;
    }

    @Override
    public boolean execute() {
        try {
            String deleteTable = "delete  from  table_def where `name` = '" + table + "'";

            log.info(deleteTable + " start!");
            MyBaseService myBaseService = MyBaseService.getInstance();
            myBaseService.delete(deleteTable);
            log.info(deleteTable + " success!");

        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("delete " + table + " from db failure", e);
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
