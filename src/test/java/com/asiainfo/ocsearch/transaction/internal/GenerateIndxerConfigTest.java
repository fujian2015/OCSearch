package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.CommonUtils;
import com.asiainfo.ocsearch.core.TableSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/4/6.
 */
public class GenerateIndxerConfigTest {
    GenerateIndxerConfig generateIndxerConfig;
    @Before
    public void setUp() throws Exception {
        generateIndxerConfig=new GenerateIndxerConfig(new TableSchema(CommonUtils.getRquestDemo()));
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void execute() throws Exception {
        generateIndxerConfig.execute();
    }

    @Test
    public void recovery() throws Exception {
        generateIndxerConfig.recovery();
    }

    @Test
    public void canExecute() throws Exception {

    }

}