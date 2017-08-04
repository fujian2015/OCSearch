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
                "    \"start\": 1,\n" +
                "    \"rows\": 5,\n" +
                "    \"rowkey_prefix\": \"e\",\n" +
                "    \"condition\":\"time>='20170716' and time<='20170718'\",\n" +
                "    \"tables\": [\n" +
                "        \"FILE__201707\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\n" +

                "    ]\n" +
                "}";
        System.out.println(request);
        System.out.println(new ScanService().query(new ObjectMapper().readTree(request)));
    }

}