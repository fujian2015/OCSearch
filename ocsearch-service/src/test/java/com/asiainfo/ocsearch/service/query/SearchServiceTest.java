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

        String request = "{\n" +
                "    \"query\": \"\",\n" +
                "    \"start\": 0,\n" +
                "    \"rows\": 20,\n" +
                "    \"sort\": \"\",\n" +
                "    \"condition\": \"*:*\",\n" +
                "    \"tables\": [\"TEST2\"],\n" +
                "    \"return_fields\": [\"id\",\"LAC\",\"CELL\"]\n" +
                "}";

        ObjectNode q=(ObjectNode)new ObjectMapper().readTree(request);
//        Set<String> phones = new HashSet<>();
        for(int i=0;i<1;i++) {
            q.put("start",10000*i);

            ArrayNode arrayNode = (ArrayNode)  new SearchService().query(q).get("docs");
            System.out.println(i+":"+arrayNode);
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