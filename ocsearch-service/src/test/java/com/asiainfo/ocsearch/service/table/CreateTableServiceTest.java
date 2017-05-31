package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/31.
 */
public class CreateTableServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        String tableString = "{\n" +
                "    \"name\": \"file__table\",\n" +
                "    \"schema\": \"fileSchema\",\n" +
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
        new CreateTableService().doService(new ObjectMapper().readTree(tableString));
//        new DeleteTableService().doService(new ObjectMapper().readTree(tableString));
    }

}