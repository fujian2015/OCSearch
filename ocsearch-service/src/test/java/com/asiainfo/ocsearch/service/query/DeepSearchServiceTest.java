package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mac on 2017/6/29.
 */
public class DeepSearchServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();

        String request = "{\"query\":\"\",\"sort\":\"id asc\",\"tables\":[\"SITE\"],\"rows\":10000,\"return_fields\":[\"id\"],\"batchs_send_cnt\":2000,\"condition\":\"id:*\",\"cursor_mark\":\"*\"}";

        ObjectNode q = (ObjectNode) new ObjectMapper().readTree(request);
        Set<String> phones = new HashSet<>();
        String mark = "*";
        int i=0;
        while (true) {
            q.put("cursor_mark", mark);

            JsonNode result = new DeepSearchService().query(q);
            System.out.println(result.get("total"));
            mark=result.get("next_cursor_mark").asText();
            ArrayNode arrayNode = (ArrayNode) result.get("docs");

            System.out.println(i+":"+arrayNode.size());
            i++;
//            arrayNode.forEach(node -> {
//                if (phones.contains(node.get("id").asText()))
//                    System.out.println(node.get("id").asText());
//                else
//                    phones.add(node.get("id").asText());
//            });
            if(arrayNode.size()<10000)
                break;
        }
        System.out.println(phones.size());
    }

}