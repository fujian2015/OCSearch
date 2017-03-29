package com.asiainfo.ocsearch.service.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import com.asiainfo.ocsearch.transaction.internal.GenerateSolrConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/3/28.
 */
public class CreateTableServiceTest {

    JsonNode request;
    @Before
    public void setUp() throws Exception {
        String json = "{\n    \"name\": \"tableName\",\n    \"storeType\": \"n\",\n    \"contentField\": \"_root_\",\n    \"contentType\": \"text\",\n    \"hbase\": {\n        \"name\": \"hbaseTable\",\n        \"isExist\": \"true\"\n    },\n    \"solr\": {\n        \"name\": \"solrCollection\",\n        \"isExist\": \"true\"\n    },\n    \"fields\": [\n        {\n            \"name\": \"length\",\n            \"type\": \"int\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"content\",\n            \"type\": \"text\",\n            \"indexed\": \"false\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"title\",\n            \"type\": \"text\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        }\n    ],\n    \"queryFields\": [\n        {\n            \"name\": \"title\",\n            \"weight\": \"10\"\n        },\n        {\n            \"name\": \"content\",\n            \"weight\": \"20\"\n        }\n    ],\n    \"baseFields\": [\n        {\n            \"name\": \"title\",\n            \"isFast\": true\n        }\n    ]\n}";
        request = (new ObjectMapper()).readTree(json);
    }

    @After
    public void tearDown() throws Exception {
        TableConfig tableConfig = new TableConfig(request);
        new GenerateSolrConfig(tableConfig).recovery();
    }

    @Test
    public void doService() throws Exception {

        CreateTableService createTableService=new CreateTableService();

        createTableService.doService(request);
    }

}