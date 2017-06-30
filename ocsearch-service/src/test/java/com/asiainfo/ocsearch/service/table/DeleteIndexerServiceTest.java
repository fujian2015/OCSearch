package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/30.
 */
public class DeleteIndexerServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        System.out.println(new ObjectMapper().readTree(new DeleteIndexerService().doService(new ObjectMapper().readTree("{\"name\":\"SITEPOSITION5\"}"))));
    }

}