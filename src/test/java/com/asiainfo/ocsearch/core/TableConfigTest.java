package com.asiainfo.ocsearch.core;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.dom4j.Element;
import org.junit.Test;

import java.util.Iterator;

public class TableConfigTest {
    public TableConfigTest() {
    }

    @Test
    public void getSolrFields() throws Exception {
        String json = "{\n    \"name\": \"tableName\",\n    \"storeType\": \"n\",\n    \"contentField\": \"_root_\",\n    \"contentType\": \"text\",\n    \"hbase\": {\n        \"name\": \"hbaseTable\",\n        \"isExist\": \"true\"\n    },\n    \"solr\": {\n        \"name\": \"solrCollection\",\n        \"isExist\": \"true\"\n    },\n    \"fields\": [\n        {\n            \"name\": \"length\",\n            \"type\": \"int\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"content\",\n            \"type\": \"text\",\n            \"indexed\": \"false\",\n            \"contented\": \"false\"\n        },\n        {\n            \"name\": \"title\",\n            \"type\": \"text\",\n            \"indexed\": \"true\",\n            \"contented\": \"false\"\n        }\n    ],\n    \"queryFields\": [\n        {\n            \"name\": \"title\",\n            \"weight\": \"10\"\n        },\n        {\n            \"name\": \"content\",\n            \"weight\": \"20\"\n        }\n    ],\n    \"baseFields\": [\n        {\n            \"name\": \"title\",\n            \"isFast\": true\n        }\n    ]\n}";
        JsonNode jsonNode = (new ObjectMapper()).readTree(json);
        TableConfig tableConfig = new TableConfig(jsonNode);
        Iterator var4 = tableConfig.getSolrFields().iterator();

        while(var4.hasNext()) {
            Element element = (Element)var4.next();
            System.out.println(element.asXML());
        }

    }
}
