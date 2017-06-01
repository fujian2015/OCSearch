package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.jdbc.pool.DbPool;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Created by mac on 2017/6/1.
 */
public class CreatePhoenixViewTest {
    @Test
    public void testExecute() throws Exception {

        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "   \"request\":true,\n" +
                "    \"name\": \"testSchema10\",\n" +
                "    \"rowkey_expression\": \"md5(phone,imsi)+‘|‘+phone+‘|‘+imsi\",\n" +
                "    \"table_expression\": \"table+’_'+time\",\n" +
                "    \"index_type\": 2,\n" +
                "    \"content_fields\": [\n" +
                "   \n" +
                "    ],\n" +
                "    \"inner_fields\": [\n" +
                "       \n" +
                "    ],\n" +
                "    \"query_fields\": [\n" +
                "      \n" +
                "    ],\n" +
                "    \"fields\": [\n" +
                "       \n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": true,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\",\n" +
                "            \"content_field\": \"_root_\",\n" +
                "            \"hbase_column\": \"1\",\n" +
                "            \"hbase_family\": \"B\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\",\n" +
                "            \"hbase_column\": \"0\",\n" +
                "            \"hbase_family\": \"B\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        Schema schema =new Schema(jsonNode);

        Properties hbase = PropertiesLoadUtil.loadXmlFile("hbase-site.xml");
        Properties druid = PropertiesLoadUtil.loadProFile("druid.properties");
        DbPool.setUp(druid,hbase);

        new CreatePhoenixView("GPRS__20170511",schema).recovery();

    }

}