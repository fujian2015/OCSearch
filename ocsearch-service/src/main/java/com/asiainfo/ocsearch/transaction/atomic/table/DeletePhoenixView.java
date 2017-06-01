package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.jdbc.phoenix.PhoenixJdbcHelper;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Created by mac on 2017/6/1.
 */
public class DeletePhoenixView implements AtomicOperation {
    static Logger log = Logger.getLogger("state");

    String table;

    public DeletePhoenixView(String table) {
        this.table = table;
    }

    @Override
    public boolean execute() {

        log.info("delete phoenix view " + table + " start!");

        boolean exists = true;
        PhoenixJdbcHelper phoenixJdbcHelper = new PhoenixJdbcHelper();
        try {
            phoenixJdbcHelper.executeQuery("select * from \"" + table + "\"");

        } catch (SQLException e) {
            exists = false;
        }
        try {
            if (exists) {
                phoenixJdbcHelper.excuteNonQuery("drop view \"" + table + "\"");
            }
        } catch (SQLException e) {
            throw new RuntimeException("delete phoenix view  failure！", e);
        } finally {
            try {
                phoenixJdbcHelper.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        log.info("delete phoenix view " + table + " success!");
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
