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
                "\"cursor_mark\":\"*\",\n" +
                "    \"start\":0,\n" +
                "    \"rows\": 3,\n" +
                "    \"rowkey_prefix\": \"1\",\n" +
                "    \"condition\":\"\",\n" +
                "    \"tables\": [\n" +
                "        \"testTable\",\"testHello\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\n" +
                "\"id\",\"other\",\"_table_\",\"name\"\n" +
                "    ]\n" +
                "}";
        System.out.println(request);
        System.out.println(new ScanService().query(new ObjectMapper().readTree(request)));
    }

}