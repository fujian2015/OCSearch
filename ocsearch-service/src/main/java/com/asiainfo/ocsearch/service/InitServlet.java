package com.asiainfo.ocsearch.service;

import com.asiainfo.ocsearch.datasource.mysql.DataSourceProvider;
import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.*;
import com.asiainfo.ocsearch.service.table.CreateTableService;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by mac on 2017/4/28.
 */
public class InitServlet implements ServletContextListener {

    Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        new InitServlet().initAll();

        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "    \"name\": \"GPRS__20140927\",\n" +
                "    \"schema\": \"testSchema\",\n" +
                "    \"hbase\": {\n" +
                "        \"region_num\": 100,\n" +
                "        \"region_split\": [\n" +
                "            \"a\",\n" +
                "            \"b\",\n" +
                "            \"c\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"solr\": {\n" +
                "        \"shards\": 10,\n" +
                "        \"replicas\": 2\n" +
                "    },\n" +
                "    \"index_type\": 0\n" +
                "}");


        try {
            new CreateTableService().doService(jsonNode);
        } catch (ServiceException e) {
            e.printStackTrace();
        }

    }

    private void initSchemaManager() throws IOException, SQLException {

        MyBaseService myBaseService = MyBaseService.getInstance();
        String queryTable = "select * from  schema_def ";

        try {
            List<Map<String, Object>> results = myBaseService.queryList(queryTable);

            for (Map<String, Object> result : results) {
                String name = String.valueOf(result.get("name"));
                String tableExpression = String.valueOf(result.get("table_expression"));
                String rowkeyExpression = String.valueOf(result.get("rowkey_expression"));

                Schema schema = new Schema(name, tableExpression, rowkeyExpression);

                ObjectMapper objectMapper = new ObjectMapper();

                String contentField = String.valueOf(result.get("content_field"));
                if (StringUtils.isNotEmpty(contentField)) {
                    schema.setContentField(new ContentField(objectMapper.readTree(contentField)));
                }

                String queryFieldsString = String.valueOf(result.get("query_fields"));

                if (StringUtils.isNotEmpty(queryFieldsString)) {

                    List<QueryField> queryFields = new ArrayList<>();
                    ArrayNode queryFieldsNode = (ArrayNode) objectMapper.readTree(queryFieldsString);
                    for (JsonNode node : queryFieldsNode) {
                        queryFields.add(new QueryField(node));
                    }
                    schema.setQueryFields(queryFields);
                }
                schema.setFields(getFields(name, myBaseService));

                SchemaManager.addSchema(name, schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("init schema manager error!", e);
            throw e;
        }

    }

    private Map<String, Field> getFields(String schema, MyBaseService myBaseService) throws SQLException {

        String queryField = "select * from  field_def where `schema_name` = '" + schema + "'";

        List<Map<String, Object>> fieldResult = myBaseService.queryList(queryField);

        Map<String, Field> fields = new HashMap<>();

        for (Map<String, Object> map : fieldResult) {
            String name = String.valueOf(map.get("name"));
            boolean indexed = Boolean.valueOf((String) map.get("indexed"));
            boolean indexContented = Boolean.valueOf((String) map.get("index_contented"));
            boolean indexStored = Boolean.valueOf((String) map.get("index_stored"));
            String indexType = String.valueOf(map.get("index_type"));
            String hbaseColumn = String.valueOf(map.get("hbase_column"));
            String hbaseFamily = String.valueOf(map.get("hbase_family"));
            String storeType = String.valueOf(map.get("store_type"));
            fields.put(name, new Field(name, indexed, indexContented, indexStored, indexType, storeType, hbaseColumn, hbaseFamily));
        }
        return fields;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {

        servletContextEvent.getServletContext().log("begin init ocsearch...");


        servletContextEvent.getServletContext().log("end init ocsearch...");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }


    public void initAll() throws Exception {

        logger.info("begin init ocsearch...");

        Properties properties = PropertiesLoadUtil.loadProFile("ocsearch.properties");

        DataSourceProvider.setUp(properties);

        initSchemaManager();

//        HbaseServiceManager.setup(HBaseConfiguration.create());
//
//        OCSearchEnv.setUp(properties);

//        boolean hbaseOnly=Boolean.valueOf(OCSearchEnv.getEnvValue("HBASE_ONLY"));
//
//        if(!hbaseOnly){
//            SolrServer.setUp(properties);
//        }

        logger.info("end init ocsearch...");

    }

}
