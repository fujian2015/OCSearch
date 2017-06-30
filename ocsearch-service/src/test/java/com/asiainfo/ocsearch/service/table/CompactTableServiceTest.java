package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/29.
 */
public class CompactTableServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        String tableString = "{\n" +
                "    \"name\": \"SITE\"" +
                "}";
        System.out.println(tableString);
        new CompactTableService().doService(new ObjectMapper().readTree(tableString));
    }

}