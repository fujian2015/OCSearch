package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.listener.SystemListener;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/7/11.
 */
public class ListSchemaServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        SolrServerManager.getInstance().optimize();
//       System.out.println(new ObjectMapper().readTree( new ListSchemaService().doService(JsonNodeFactory.instance.objectNode())));
    }

}