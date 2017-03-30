package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

/**
 * Created by mac on 2017/3/30.
 */
public class SaveConfigToDbTest {
    SaveConfigToDb saveConfigToDb;

    @Before
    public void setUp() throws Exception {

        String json = "{\n" +
                "    \"name\": \"tableName\",\n" +
                "    \"store\": {\n" +
                "        \"type\": \"d\",\n" +
                "        \"period\": 30,\n" +
                "        \"partition\": \"title\"\n" +
                "    },\n" +
                "    \"content\": {\n" +
                "        \"name\": \"_root_\",\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"rowkey\": {\n" +
                "        \"version\": 1,\n" +
                "        \"keys\": [\n" +
                "            {\n" +
                "                \"name\": \"title\",\n" +
                "                \"order\": 0\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"hbase\": {\n" +
                "        \"name\": \"hbaseTable\",\n" +
                "        \"isExist\": \"true\"\n" +
                "    },\n" +
                "    \"solr\": {\n" +
                "        \"name\": \"solrCollection\",\n" +
                "        \"isExist\": \"true\"\n" +
                "    },\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"length\",\n" +
                "            \"type\": \"int\",\n" +
                "            \"indexed\": \"true\",\n" +
                "            \"contented\": \"false\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"type\": \"text\",\n" +
                "            \"indexed\": \"false\",\n" +
                "            \"contented\": \"false\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"type\": \"text\",\n" +
                "            \"indexed\": \"true\",\n" +
                "            \"contented\": \"false\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"queryfields\": [\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"weight\": 10\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"weight\": 20\n" +
                "        }\n" +
                "    ],\n" +
                "    \"basefields\": [\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"isFast\": true\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        JsonNode jsonNode = (new ObjectMapper()).readTree(json);
        saveConfigToDb = new SaveConfigToDb(new TableConfig(jsonNode));

    }

    @After
    public void tearDown() throws Exception {
//        recovery();
    }

    @Test
    public void execute() throws Exception {
        Assert.assertEquals(this.saveConfigToDb.execute(), true);
    }

    @Test
    public void recovery() throws Exception {
        Assert.assertEquals(this.saveConfigToDb.recovery(), true);
    }

}