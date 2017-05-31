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
                "\t\"request\":true,\n" +
                "    \"name\": \"fileSchema\",\n" +
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
                "            \"name\": \"picture\",\n" +
                "            \"indexed\": false,\n"  +
                "            \"store_type\": \"FILE\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");

        System.out.println(new Schema(jsonNode));
        new AddSchemaService().doService(jsonNode);

    }


}