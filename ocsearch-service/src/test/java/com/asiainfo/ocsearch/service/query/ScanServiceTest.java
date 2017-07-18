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
                "    \"start\": 0,\n" +
                "    \"rows\": 60,\n" +
                "    \"rowkey_prefix\": \"4\",\n" +
                "    \"condition\":\"CELL=='61891'\",\n" +
                "    \"tables\": [\n" +
                "        \"SITE2\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\n" +
                "    \"PHONENUM\"\n" +
                "    ]\n" +
                "}";
        System.out.println(request);
        System.out.println(new ScanService().query(new ObjectMapper().readTree(request)));
    }

}