package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.CommonUtils;
import com.asiainfo.ocsearch.core.TableSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GenerateSolrConfigTest {

    GenerateSolrConfig generateSolrConfig;

    @After
    public void tearDown() throws Exception {
        this.generateSolrConfig.recovery();
    }

    @Before
    public void setUp() throws Exception {

        TableSchema tableSchema = new TableSchema(CommonUtils.getRquestDemo());
        this.generateSolrConfig = new GenerateSolrConfig(tableSchema);
    }

    @Test
    public void execute() throws Exception {
        Assert.assertEquals(Boolean.valueOf(true), Boolean.valueOf(this.generateSolrConfig.execute()));
    }

    @Test
    public void recovery() throws Exception {
        Assert.assertEquals(Boolean.valueOf(true), Boolean.valueOf(this.generateSolrConfig.recovery()));
    }
}

