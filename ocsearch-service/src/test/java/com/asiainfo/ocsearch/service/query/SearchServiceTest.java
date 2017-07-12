package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/25.
 */
public class SearchServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();

        String request = "{\"query\":\"\",\"sort\":\"id asc\",\"tables\":[\"SITE\"],\"rows\":10000,\"return_fields\":[\"id\"],\"batchs_send_cnt\":2000,\"condition\":\"id:*\",\"nextCursor\":\"*\"}";

        ObjectNode q=(ObjectNode)new ObjectMapper().readTree(request);
//        Set<String> phones = new HashSet<>();
        for(int i=0;i<1000;i++) {
            q.put("start",10000*i);

            ArrayNode arrayNode = (ArrayNode)  new SearchService().query(q).get("docs");
            System.out.println(i+":"+arrayNode.size());
//            if(arrayNode.size()<10000)
//                break;

//            arrayNode.forEach(node -> {
//                if (phones.contains(node.get("id").asText()))
//                    System.out.println(node.get("id").asText());
//                else
//                    phones.add(node.get("id").asText());
//            });

        }



    }

}