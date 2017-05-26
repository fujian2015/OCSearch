package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/26.
 */
public class ScanServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();
        String request = "{\n" +
                "    \"row\": \"content\",\n" +
                "    \"start\": 0,\n" +
                "    \"rows\": 60,\n" +
                "    \"rowkey_prefix\": \"hahed\",\n" +
                "    \"condition\": \"title=='content'\",\n" +
                "    \"tables\": [\n" +
                "        \"GPRS__20170510\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\n" +
                "        \"title\"\n" +
                "    ]\n" +
                "}";
        System.out.println(new ScanService().query(new ObjectMapper().readTree(request)));
    }

}