package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.jdbc.phoenix.PhoenixJdbcHelper;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Created by mac on 2017/6/1.
 */
public class CreatePhoenixView implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String table;
    Schema schema;

    public CreatePhoenixView(String table, Schema schema) {

        this.table = table;
        this.schema = schema;
    }

    @Override
    public boolean execute() {

        log.info("create phoenix view " + table + " start!");

        String sql = this.constructSql();
        PhoenixJdbcHelper phoenixJdbcHelper = new PhoenixJdbcHelper();
        try {
            phoenixJdbcHelper.excuteNonQuery(sql);
        } catch (SQLException e) {
            log.error("create phoenix view failure!", e);
            throw new RuntimeException("create phoenix table failure!", e);
        }finally {
            try {
                phoenixJdbcHelper.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        log.info("create phoenix table " + table + " success!");

        return true;
    }

    private String constructSql() {
        StringBuilder sb = new StringBuilder("create view \"");
        sb.append(table);
        sb.append("\"(\"id\" varchar primary key");
        schema.getFields().values().forEach(
                field -> {
                    sb.append(",\"");
                    sb.append(field.getHbaseFamily());
                    sb.append("\".\"");
                    sb.append(field.getHbaseColumn());
                    sb.append("\" ");
                    sb.append(getType(field.getStoreType()));
                }

        );
        sb.append(")");
        System.out.println(sb.toString());
        return sb.toString();
    }

    private String getType(FieldType storeType) {
        switch (storeType) {
            case INT:
                return "INTEGER";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR";
            default: //any more store type?
                throw new RuntimeException("unexpected store type:" + storeType.toString() + " for phoenix!");
        }
    }

    @Override
    public boolean recovery() {

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
    public boolean canExecute() {

        log.info("check phoenix view" + table + " start!");
        boolean exists = true;
        PhoenixJdbcHelper phoenixJdbcHelper = new PhoenixJdbcHelper();
        try {
            phoenixJdbcHelper.executeQuery("select * from \"" + table + "\"");
        } catch (SQLException e) {
            exists = false;
        }finally {
            try {
                phoenixJdbcHelper.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        log.info("check phoenix view " + table + " success!");
        return !exists;
    }
}
