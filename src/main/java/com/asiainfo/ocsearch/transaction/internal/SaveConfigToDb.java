package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import com.asiainfo.ocsearch.db.mysql.MyBaseService;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by mac on 2017/3/29.
 */
public class SaveConfigToDb implements AtomicOperation, Serializable {

    TableConfig tableConfig;

    transient MyBaseService myBaseService;

    public SaveConfigToDb(TableConfig tableConfig) {

        this.tableConfig = tableConfig;
        this.myBaseService = MyBaseService.getInstance();
    }

    public boolean execute() {


        try {

            Map<String, Object> tableMap = tableConfig.getTableFields();

            String insertTable = prepareSql("table_def", tableMap.keySet());

            myBaseService.insertOrUpdate(insertTable, tableMap.values().toArray());


            List<Map<String, Object>> schemaList = tableConfig.getSchemaFields();

            String insertSchema = prepareSql("schema_def", schemaList.get(0).keySet());

            myBaseService.batch(insertSchema, prepareObjects(schemaList));


            List<Map<String, Object>> baseFields = tableConfig.getBaseFields();
            if (!baseFields.isEmpty()) {
                String insertBase = prepareSql("base_def", baseFields.get(0).keySet());
                myBaseService.batch(insertBase, prepareObjects(baseFields));
            }

            List<Map<String, Object>> queryFields = tableConfig.getQueryFields();
            if (!queryFields.isEmpty()) {
                String insertQuery = prepareSql("query_def", queryFields.get(0).keySet());
                myBaseService.batch(insertQuery, prepareObjects(queryFields));
            }

            List<Map<String, Object>> keyFields = tableConfig.getKeyFields();
            if (!keyFields.isEmpty()) {
                String insertkey = prepareSql("rowkey_def", keyFields.get(0).keySet());
                myBaseService.batch(insertkey, prepareObjects(keyFields));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("insert " + tableConfig.name + " to db failure", e);
        }

        return true;
    }

    private Object[][] prepareObjects(List<Map<String, Object>> schemaList) {

        Object[][] fields = new Object[schemaList.size()][];
        int i = 0;

        for (Map<String, Object> fieldMap : schemaList) {
            fields[i++] = fieldMap.values().toArray();
        }
        return fields;
    }

    private String prepareSql(String table, Set<String> keys) {
        StringBuilder sb = new StringBuilder("insert into ");
        sb.append(table);
        sb.append("(");
        sb.append(StringUtils.join(keys, ","));
        sb.append(")values(");
        String[] quots = new String[keys.size()];
        Arrays.fill(quots, "?");
        sb.append(StringUtils.join(quots, ","));
        sb.append(");");
        return sb.toString();
    }


    public boolean recovery() {

        String deleteTable = "delete  from  table_def where `name` = '" + tableConfig.name + "'";
        String deleteSchema = "delete  from  schema_def where `table_name` = '" + tableConfig.name + "'";
        String deleteBase = "delete from  base_def where `table_name` = '" + tableConfig.name + "'";
        String deleteQuery = "delete from  query_def where `table_name` = '" + tableConfig.name + "'";
        String deleteRowKey = "delete from  rowkey_def where `table_name` = '" + tableConfig.name + "'";

        try {
            myBaseService.delete(deleteRowKey);
            myBaseService.delete(deleteQuery);
            myBaseService.delete(deleteBase);
            myBaseService.delete(deleteSchema);
            myBaseService.delete(deleteTable);

        } catch (SQLException e) {
            throw new RuntimeException("delete " + tableConfig.name + " from db failure", e);
        }
        return true;
    }
}
