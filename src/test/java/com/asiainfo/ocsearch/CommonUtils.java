package com.asiainfo.ocsearch;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by mac on 2017/4/6.
 */
public class CommonUtils {
    public static JsonNode getRquestDemo() throws IOException {
        String json ="{\n" +
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
                "        \"exist\": \"true\",\n" +
                "        \"regions\": 100\n" +
                "    },\n" +
                "    \"solr\": {\n" +
                "        \"name\": \"solrCollection\",\n" +
                "        \"exist\": \"true\",\n" +
                "        \"shards\": 20\n" +
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
        System.out.println(json);
       return (new ObjectMapper()).readTree(json);
    }
}
