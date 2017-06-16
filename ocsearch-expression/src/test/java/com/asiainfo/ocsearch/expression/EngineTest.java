package com.asiainfo.ocsearch.expression;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/5/11.
 */
public class EngineTest {
    @Test
    public void testCreateExecutor() throws Exception {
        Map<String, Object> jc = new HashMap<>();

        jc.put("phone", "13800000000");

        jc.put("imsi", "sdgsdgsdg");

        jc.put("pi","20170611");


//        Executor ee = Engine.getInstance().createExecutor("phone+‘_‘+imsi");

        Executor ee = Engine.getInstance().createExecutor("'GPRS__'+ $date:format($date:todate(pi,'yyyyMMdd').getTime(),'yyyyMMddHH')");

        System.out.println(ee.evaluate(jc));
    }

}