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
                "\"request\":true,\n" +
                "    \"name\": \"testSchema10\",\n" +
                "    \"rowkey_expression\": \"md5(phone,imsi)+‘|‘+phone+‘|‘+imsi\",\n" +
                "    \"table_expression\": \"table+’_'+time\",\n" +
                "    \"index_type\": 0,\n" +
                "    \"content_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"_root_\",\n" +
                "            \"type\": \"text_gl\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"inner_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"basic\",\n" +
                "            \"separator\": \";\"\n" +
                "        }\n" +
                "    ],\n" +
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
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"int\",\n" +
                "            \"store_type\": \"INT\",\n" +
                "            \"content_field\": \"_root_\",\n" +
                "            \"inner_field\": \"basic\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": true,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\",\n" +
                "            \"content_field\": \"_root_\",\n" +
                "            \"inner_field\": \"basic\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"indexed\": false,\n" +
                "            \"store_type\": \"STRING\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        Schema schema=new Schema(jsonNode);

        Schema a= (Schema) schema.clone();
        System.out.println("GPRS_123".hashCode());
        System.out.println("GPRS_124".hashCode());
        System.out.println("GPRS_124aaa".hashCode());
    }

}