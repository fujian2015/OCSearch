package com.asiainfo.ocsearch.service.expression;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/12.
 */
public class ListExpressionServiceTest {
    @Test
    public void testDoService() throws Exception {

        JsonNode jsonNode = new ObjectMapper().readTree("{}");

        System.out.println(new ObjectMapper().readTree(new ListExpressionService().doService(jsonNode)));
    }

}