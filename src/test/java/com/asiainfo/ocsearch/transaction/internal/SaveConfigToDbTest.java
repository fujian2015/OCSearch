package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.CommonUtils;
import com.asiainfo.ocsearch.core.TableSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

/**
 * Created by mac on 2017/3/30.
 */
public class SaveConfigToDbTest {
    SaveConfigToDb saveConfigToDb;

    @Before
    public void setUp() throws Exception {
        saveConfigToDb = new SaveConfigToDb(new TableSchema(CommonUtils.getRquestDemo()));
    }

    @After
    public void tearDown() throws Exception {
        recovery();
    }

    @Test
    public void execute() throws Exception {
        Assert.assertEquals(this.saveConfigToDb.execute(), true);
    }

    @Test
    public void recovery() throws Exception {
        Assert.assertEquals(this.saveConfigToDb.recovery(), true);
    }

}