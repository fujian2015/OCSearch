package com.asiainfo.ocsearch.expression.namespace;

import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/5/11.
 */
public class DateNameSpaceTest {
    @Test
    public void testFormat() throws Exception {

    }

    @Test
    public void testToDate() throws Exception {

    }

    @Test
    public void testNow() throws Exception {
        Map<String, Object> jc = new HashMap<>();


        Executor ee = Engine.getInstance().createExecutor("'GPRS__'+ $date:now('yyyyMMdd')");

        System.out.println(ee.evaluate(jc));
    }

}