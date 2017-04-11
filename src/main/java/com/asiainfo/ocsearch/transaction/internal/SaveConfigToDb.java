package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableSchema;
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

    TableSchema tableSchema;


    public SaveConfigToDb(TableSchema tableSchema) {

        this.tableSchema = tableSchema;
    }

    public boolean execute() {


        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            Map<String, Object> tableMap = tableSchema.getTableFields();

            String insertTable = prepareSql("table_def", tableMap.keySet());

            myBaseService.insertOrUpdate(insertTable, tableMap.values().toArray());


            List<Map<String, Object>> schemaList = tableSchema.getSchemaFields();

            String insertSchema = prepareSql("schema_def", schemaList.get(0).keySet());

            myBaseService.batch(insertSchema, prepareObjects(schemaList));


            List<Map<String, Object>> baseFields = tableSchema.getBaseFields();
            if (!baseFields.isEmpty()) {
                String insertBase = prepareSql("base_def", baseFields.get(0).keySet());
                myBaseService.batch(insertBase, prepareObjects(baseFields));
            }

            List<Map<String, Object>> queryFields = tableSchema.getQueryFields();
            if (!queryFields.isEmpty()) {
                String insertQuery = prepareSql("query_def", queryFields.get(0).keySet());
                myBaseService.batch(insertQuery, prepareObjects(queryFields));
            }

            List<Map<String, Object>> keyFields = tableSchema.getKeyFields();
            if (!keyFields.isEmpty()) {
                String insertkey = prepareSql("rowkey_def", keyFields.get(0).keySet());
                myBaseService.batch(insertkey, prepareObjects(keyFields));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("insert " + tableSchema.name + " to db failure", e);
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

        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String deleteTable = "delete  from  table_def where `name` = '" + tableSchema.name + "'";
            String deleteSchema = "delete  from  schema_def where `table_name` = '" + tableSchema.name + "'";
            String deleteBase = "delete from  base_def where `table_name` = '" + tableSchema.name + "'";
            String deleteQuery = "delete from  query_def where `table_name` = '" + tableSchema.name + "'";
            String deleteRowKey = "delete from  rowkey_def where `table_name` = '" + tableSchema.name + "'";
            myBaseService.delete(deleteRowKey);
            myBaseService.delete(deleteQuery);
            myBaseService.delete(deleteBase);
            myBaseService.delete(deleteSchema);
            myBaseService.delete(deleteTable);

        } catch (SQLException e) {
            throw new RuntimeException("delete " + tableSchema.name + " from db failure", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String queryTable = "select * from  table_def where `name` = '" + tableSchema.name + "'";

            Map<String,Object> result=myBaseService.queryOne(queryTable);

            if(result==null||result.isEmpty())
                return true;
        } catch (SQLException e) {
            throw new RuntimeException("delete " + tableSchema.name + " from db failure", e);
        }
        return false;
    }
}
