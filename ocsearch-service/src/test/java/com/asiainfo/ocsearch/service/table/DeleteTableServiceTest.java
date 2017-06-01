package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/1.
 */
public class DeleteTableServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        String tableString = "{\n" +
                "    \"name\": \"phoenixTable\",\n" +
                "    \"schema\": \"phoenixSchema\",\n" +
                "    \"hbase\": {\n" +
                "        \"region_num\": 10,\n" +
                "        \"region_split\": [\n" +
                "            \n" +
                "        ]\n" +
                "    },\n" +
                "    \"solr\": {\n" +
                "        \"shards\": 2,\n" +
                "        \"replicas\": 2\n" +
                "    }\n" +
                "}";
        System.out.println(tableString);
        new DeleteTableService().doService(new ObjectMapper().readTree(tableString));
    }

}