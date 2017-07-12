package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/7/11.
 */
public class UpdateIndexerServiceTest {
    @Test
    public void testDoService() throws Exception {
       new  SystemListener().initAll();
        System.out.println(new ObjectMapper().readTree(new UpdateIndexerService().doService(new ObjectMapper().readTree("{\"name\":\"SITEPOSITION\"}"))));

    }

}