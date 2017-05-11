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

        jc.put("pi", "20170511");

        Executor ee = Engine.getInstance().createExecutor("'GPRS__'+ $date:format($date:todate(pi,'yyyyMMdd').getTime(),'yyyyMMddHH')");

        System.out.println(ee.evaluate(jc));
    }

}