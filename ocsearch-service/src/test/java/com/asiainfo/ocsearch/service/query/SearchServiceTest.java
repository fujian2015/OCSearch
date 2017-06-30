package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mac on 2017/5/25.
 */
public class SearchServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();

        String request = "{\"query\":\"\",\"sort\":\"id asc\",\"tables\":[\"SITE\"],\"rows\":100,\"return_fields\":[\"id\"],\"batchs_send_cnt\":2000,\"condition\":\"id:*\",\"nextCursor\":\"*\"}";

        ObjectNode q=(ObjectNode)new ObjectMapper().readTree(request);
        Set<String> phones = new HashSet<>();
        for(int i=0;i<1;i++) {
            q.put("start",100*(1-i-1));

            ArrayNode arrayNode = (ArrayNode)  new SearchService().query(q).get("docs");

            arrayNode.forEach(node -> {
                if (phones.contains(node.get("id").asText()))
                    System.out.println(node.get("id").asText());
                else
                    phones.add(node.get("id").asText());
            });
        }
        System.out.println(phones.size());


    }

}