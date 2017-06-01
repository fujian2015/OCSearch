package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/6/1.
 */
public class SqlServiceTest {
    @Test
    public void testQuery() throws Exception {
        new SystemListener().initAll();


     SqlService sqlService=new SqlService();

//        getService.execute("GPRS__20170510",t->t.put());

        String request="{\n" +
                "    \"sql\": \"select * from \\\"phoenixTable\\\"\"\n" +
                "}";
//        System.out.println(new ObjectMapper().readTree(request));
        System.out.println(sqlService.query(new ObjectMapper().readTree(request)));
    }

}