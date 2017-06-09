package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/2.
 */
public class BatchIndexServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();

//        SolrServerManager.getInstance().deleteByQuery("GPRS__20170510","id:*");
        String request = "{\n" +
                "    \"table\": \"GPRS__20170510\",\n" +
                "    \"ids\": [\n" +
                "        \"hahed3\",\n" +
                "        \"hahed2\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\"id\",\"length\"]\n" +
                "}";
//        System.out.println(request);

        new BatchIndexService().doService(new ObjectMapper().readTree(request));
//        System.out.println(IndexerServiceManager.getIndexerService().getConfiguration());
    }

}