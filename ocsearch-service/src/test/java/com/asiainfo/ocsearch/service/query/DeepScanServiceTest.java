package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/29.
 */
public class DeepScanServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();
        String request = "{\n" +
                "    \"start\": 0,\n" +
                "    \"rows\": 1000,\n" +
                "    \"rowkey_prefix\": \"b3\",\n" +
                "    \"condition\": \"SECURITY_AREA=='A0A211017' or LAC==''\",\n" +
                "    \"tables\": [\n" +
                "        \"SITE\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\n" +
                "    ]\n" +
                "}";
        System.out.println(request);
//        System.out.println(new ScanService().query(new ObjectMapper().readTree(request)));
        ObjectNode q = (ObjectNode) new ObjectMapper().readTree(request);
        String mark = "*";
        int total = 0;
        while (true) {
            q.put("cursor_mark", mark);

            JsonNode result = new DeepScanService().query(q);

            mark = result.get("next_cursor_mark").asText();
            ArrayNode arrayNode = (ArrayNode) result.get("docs");
            total += arrayNode.size();
            if (arrayNode.size() < 1000)
                break;
            System.out.println(arrayNode.get(0));
        }
        System.out.println(total);
    }

}