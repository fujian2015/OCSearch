package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/25.
 */
public class SearchServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();
        String request = "{\n" +
                "    \"query\": \"content\",\n" +
                "    \"start\": 0,\n" +
                "    \"rows\": 60,\n" +
                "    \"sort\": \"id desc\",\n" +
                "    \"condition\": \"title:*\",\n" +
                "    \"tables\": [\"GPRS__20170510\"],\n" +
                "    \"ids\": [\n" +
                "        \"hahed3\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\"length\"]\n" +
                "}";
        System.out.println(new SearchService().query(new ObjectMapper().readTree(request)));

    }

}