package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GenerateSolrConfigTest {
    GenerateSolrConfig generateSolrConfig;

    public GenerateSolrConfigTest() {
    }

    @After
    public void tearDown() throws Exception {
        this.generateSolrConfig.recovery();
    }

    @Before
    public void setUp() throws Exception {
        String json = "{\n    \"name\": \"tableName\",\n    \"storeType\": \"n\",\n    \"contentField\": \"_root_\",\n    \"contentType\": \"text\",\n    \"hbase\": {\n        \"name\": \"hbaseTable\",\n        \"isExist\": \"true\"\n    },\n    \"solr\": {\n        \"name\": \"solrCollection\",\n        \"isExist\": \"true\"\n    },\n    \"fields\": [\n        {\n            \"name\": \"length\",\n            \"type\": \"int\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"content\",\n            \"type\": \"text\",\n            \"indexed\": \"false\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"title\",\n            \"type\": \"text\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        }\n    ],\n    \"queryFields\": [\n        {\n            \"name\": \"title\",\n            \"weight\": \"10\"\n        },\n        {\n            \"name\": \"content\",\n            \"weight\": \"20\"\n        }\n    ],\n    \"baseFields\": [\n        {\n            \"name\": \"title\",\n            \"isFast\": true\n        }\n    ]\n}";
        JsonNode jsonNode = (new ObjectMapper()).readTree(json);
        TableConfig tableConfig = new TableConfig(jsonNode);
        this.generateSolrConfig = new GenerateSolrConfig(tableConfig);
    }

    @Test
    public void execute() throws Exception {
        Assert.assertEquals(Boolean.valueOf(true), Boolean.valueOf(this.generateSolrConfig.execute()));
    }

    @Test
    public void recovery() throws Exception {
        Assert.assertEquals(Boolean.valueOf(true), Boolean.valueOf(this.generateSolrConfig.recovery()));
    }
}

