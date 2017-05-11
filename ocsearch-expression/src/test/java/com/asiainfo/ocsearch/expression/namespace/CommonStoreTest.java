package com.asiainfo.ocsearch.expression.namespace;

import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/5/11.
 */
public class CommonStoreTest {
    @Test
    public void testUuid() throws Exception {
        Map<String, Object> jc = new HashMap<>();

        Executor ee = Engine.getInstance().createExecutor("$common:uuid()");

        System.out.println(ee.evaluate(jc));
    }

    @Test
    public void testNextInt() throws Exception {
        Map<String, Object> jc = new HashMap<>();

        Executor ee = Engine.getInstance().createExecutor("$common:nextInt(10)+''");

        System.out.println(ee.evaluate(jc));
    }

}