package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.listener.SystemListener;
import com.asiainfo.ocsearch.meta.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/31.
 */
public class AddSchemaServiceTest {
    @Test
    public void testQuery() throws Exception {
        testAddSchema();

    }

    public static void testAddSchema() throws Exception {

        new SystemListener().initAll();
        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "\t\"with_hbase\":true,\n" +
                "    \"name\": \"schemaShanXi1\",\n" +
                "    \"rowkey_expression\": \"phone+‘|‘+imsi\",\n" +
                "    \"table_expression\": \"table+’_'+time\",\n" +
                "    \"index_type\": 0,\n" +
                "    \"content_fields\": [\n" +
                "        \n" +
                "    ],\n" +
                "    \"inner_fields\": [\n" +
                "       \n" +
                "    ],\n" +
                "    \"query_fields\": [\n" +
                "       \n" +
                "    ],\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"NAME\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"string\",\n" +
                "            \"store_type\": \"string\",\n" +
                "            \"hbase_column\":\"NAME\",\n" +
                "            \"hbase_family\":\"0\"\n" +
                "        },\n" +
                "         {\n" +
                "            \"name\": \"AGE\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"string\",\n" +
                "            \"store_type\": \"string\",\n" +
                "            \"hbase_column\":\"AGE\",\n" +
                "            \"hbase_family\":\"0\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");

        System.out.println(new Schema(jsonNode));
//        new AddSchemaService().doService(jsonNode);

    }


}