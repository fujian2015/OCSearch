package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/7/11.
 */
public class ListSchemaServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
       System.out.println(new ObjectMapper().readTree( new ListSchemaService().doService(JsonNodeFactory.instance.objectNode())));
    }

}