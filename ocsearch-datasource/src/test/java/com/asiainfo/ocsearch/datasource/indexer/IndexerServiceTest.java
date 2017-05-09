package com.asiainfo.ocsearch.datasource.indexer;

import org.testng.annotations.Test;

/**
 * Created by mac on 2017/5/4.
 */
public class IndexerServiceTest {
    @Test
    public void testCreateTable() throws Exception {
        new IndexerService("").createTable("table","conf");
    }

}