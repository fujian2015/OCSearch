package com.asiainfo.ocsearch.utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

/**
 * Created by mac on 2017/4/6.
 */
public class JsonWirterUtilTest {
    @Test
    public void toConfigString() throws Exception {
        String json = "[\n" +
                "    {\n" +
                "        \"id\": \"morphline1\",\n" +
                "        \"importCommands\": [\n" +
                "            \"org.kitesdk.morphline.**\",\n" +
                "            \"com.ngdata.**\"\n" +
                "        ],\n" +
                "        \"commands\": [\n" +
                "            {\n" +
                "                \"extractHBaseCells\": {\n" +
                "                    \"mappings\": [\n" +
                "                        {\n" +
                "                            \"inputColumn\": \"info:firstname\",\n" +
                "                            \"outputField\": \"firstname\",\n" +
                "                            \"type\": \"string\",\n" +
                "                            \"source\": \"value\"\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"inputColumn\": \"info:lastname\",\n" +
                "                            \"outputField\": \"lastname\",\n" +
                "                            \"type\": \"string\",\n" +
                "                            \"source\": \"value\"\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"inputColumn\": \"info:descwus\",\n" +
                "                            \"outputField\": \"descwus\",\n" +
                "                            \"type\": \"string\",\n" +
                "                            \"source\": \"value\"\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"inputColumn\": \"info:address\",\n" +
                "                            \"outputField\": \"address\",\n" +
                "                            \"type\": \"com.ngdata.hbaseindexer.parse.JsonByteArrayValueMapper\",\n" +
                "                            \"source\": \"value\"\n" +
                "                        }\n" +
                "                    ]\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"logTrace\": {\n" +
                "                    \"format\": \"output record: {}\",\n" +
                "                    \"args\": [\n" +
                "                        \"@{}\"\n" +
                "                    ]\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "]";
        JsonNode jsonNode = (new ObjectMapper()).readTree(json);
        System.out.println(JsonWirterUtil.toConfigString(jsonNode,0));

    }

}