package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/27.
 */
public class ListTableServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        System.out.println(new ObjectMapper().readTree(new ListTableService().doService(JsonNodeFactory.instance.objectNode())));
    }

}