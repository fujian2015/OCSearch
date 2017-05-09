package com.asiainfo.ocsearch.meta;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/4/19.
 */
public class SchemaTest {
    @Test
    public void testToString() throws Exception {
        JsonNode jsonNode=new ObjectMapper().readTree("{\n" +
                "    \"name\": \"testSchema\",\n" +
                "    \"rowkey_expression\": \"md5(phone,imsi)+‘|‘+phone+‘|‘+imsi\",\n" +
                "    \"table_expression\": \"table+’_'+time\",\n" +
                "    \"index_type\": 0,\n" +
                "    \"content_field\": {\n" +
                "        \"name\": \"_root_\",\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"query_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"weight\": 10\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"weight\": 20\n" +
                "        }\n" +
                "    ],\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"length\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_contented\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"int\",\n" +
                "            \"hbase_column\": \"length\",\n" +
                "            \"hbase_family\": \"B\",\n" +
                "            \"store_type\": \"INT\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_contented\": true,\n" +
                "            \"index_stored\": true,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"hbase_column\": \"title\",\n" +
                "            \"hbase_family\": \"B\",\n" +
                "            \"store_type\": \"STRING\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"indexed\": false,\n" +
                "            \"index_contented\": false,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"hbase_column\": \"content\",\n" +
                "            \"hbase_family\": \"B\",\n" +
                "            \"store_type\": \"STRING\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        Schema schema=new Schema(jsonNode);


        Schema a= (Schema) schema.clone();
        schema.setContentField(null);
        System.out.println(a);
    }

}