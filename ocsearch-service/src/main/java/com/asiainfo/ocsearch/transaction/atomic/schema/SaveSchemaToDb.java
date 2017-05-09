package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.QueryField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.SqlUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by mac on 2017/3/29.
 */
public class SaveSchemaToDb implements AtomicOperation {

    static  Logger logger = Logger.getLogger("state");

    Schema tableSchema;

    public SaveSchemaToDb(Schema tableSchema) {

        this.tableSchema = tableSchema;
    }

    public boolean execute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            Map<String, Object>  schemaMap = generateSchema();

            String insertSchema = SqlUtil.prepareSql("schema_def", schemaMap.keySet());

            myBaseService.insertOrUpdate(insertSchema, schemaMap.values().toArray());

            List<Map<String, Object>> schemaList = generateSchemaFields();

            String insertField = SqlUtil.prepareSql("field_def", schemaList.get(0).keySet());

            myBaseService.batch(insertField, SqlUtil.prepareObjects(schemaList));

        } catch (SQLException e) {
            logger.error(e);
            throw new RuntimeException("insert " + tableSchema.name + " to db failure", e);
        }

        return true;
    }


    public Map<String, Object> generateSchema() {

        Map<String, Object> schemaMap = new TreeMap();
        schemaMap.put("name", tableSchema.getName());
        schemaMap.put("rowkey_expression", tableSchema.getRowkeyExpression());
        schemaMap.put("index_type", tableSchema.getIndexType().getValue());
        schemaMap.put("table_expression", tableSchema.getTableExpression());
        if(tableSchema.getContentField()!=null)
            schemaMap.put("content_field", tableSchema.getContentField().toString());

        ArrayNode queryNodes= JsonNodeFactory.instance.arrayNode();
        for(QueryField qf:tableSchema.getQueryFields()){
            queryNodes.add(qf.toJsonNode());
        }
        schemaMap.put("query_fields", queryNodes.toString());

        return schemaMap;
    }

    /**
     * `name` varchar(255) NOT NULL,
     * `indexed` varchar(5) NOT NULL,
     * `index_contented` varchar(5) NOT NULL,
     * `index_stored` varchar(5) NOT NULL,
     * `index_type` varchar(255) NOT NULL,
     * `hbase_column` varchar(255) NOT NULL,
     * `hbase_family` varchar(255) NOT NULL,
     * `store_type` varchar(255) NOT NULL,
     * `schema_name` varchar(255) NOT NULL,
     *
     * @return
     */
    public List<Map<String, Object>> generateSchemaFields() {

        Map<String, Field> fields = tableSchema.getFields();
        List<Map<String, Object>> schemas = new ArrayList<Map<String, Object>>(fields.size());

        for (Field f : fields.values()) {
            Map fieldMap = new TreeMap();
            fieldMap.put("name", f.getName());
            fieldMap.put("indexed", String.valueOf(f.isIndexed()));
            fieldMap.put("index_contented", String.valueOf(f.isIndexContented()));
            fieldMap.put("index_stored", String.valueOf(f.isIndexStored()));
            fieldMap.put("index_type", f.getIndexType());
            fieldMap.put("hbase_column", f.getHbaseColumn());
            fieldMap.put("hbase_family", f.getHbaseFamily());
            fieldMap.put("store_type", f.getStoreType().toString());
            fieldMap.put("schema_name", tableSchema.getName());
            schemas.add(fieldMap);
        }
        return schemas;
    }


    public boolean recovery() {

        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String deleteSchema = "delete  from  schema_def where `name` = '" + tableSchema.name + "'";
            String deleteField = "delete from  field_def where `schema_name` = '" + tableSchema.name + "'";

            myBaseService.delete(deleteField);
            myBaseService.delete(deleteSchema);
        } catch (SQLException e) {
            logger.error(e);
            throw new RuntimeException("delete " + tableSchema.name + " from db failure", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String queryTable = "select * from  schema_def where `name` = '" + tableSchema.name + "'";

            Map<String, Object> result = myBaseService.queryOne(queryTable);

            if (result == null || result.isEmpty())
                return true;
            logger.warn("schema " + tableSchema.getName() + " has existed!");
        } catch (SQLException e) {
            logger.error(e);
            throw new RuntimeException("check " + tableSchema.name + " from db failure", e);
        }
        return false;
    }
}
