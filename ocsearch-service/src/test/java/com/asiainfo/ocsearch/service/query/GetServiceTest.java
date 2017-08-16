package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.listener.SystemListener;
import org.apache.hadoop.hbase.client.Put;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;
/**
 * Created by mac on 2017/5/25.
 */
public class GetServiceTest {
    @Test
    public void testQuery() throws Exception {

        new SystemListener().initAll();

        Put put=new Put("hahed3".getBytes());

        com.asiainfo.ocsearch.datasource.hbase.GetService getService= HbaseServiceManager.getInstance().getGetService();

//        getService.execute("GPRS__20170510",t->t.put());

        String request="{\n" +
                "    \"tables\": [\"TEST2\"],\n" +
                "    \"ids\": [\n" +
                "        \"DzQ2MDAwMDAwMDAy\",\n" +
                "        \"DjQ2MDAwMDAwMDAx\"\n" +
                "    ],\n" +
                "    \"return_fields\": [\"id\",\"LAC\"]\n" +
                "}";
        System.out.println(request);
        System.out.println(new GetService().query(new ObjectMapper().readTree(request)));
    }

}