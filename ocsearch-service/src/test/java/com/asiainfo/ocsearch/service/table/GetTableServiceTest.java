package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/7/25.
 */
public class GetTableServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "   \"type\":\"schema\",\n" +
                "    \"name\": \"schemaYidong\"\n" +
                "}");

        System.out.println(new ObjectMapper().readTree(new GetTableService().doService(jsonNode)));
    }

}