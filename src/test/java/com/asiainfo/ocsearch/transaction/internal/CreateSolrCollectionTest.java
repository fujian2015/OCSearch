package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.db.solr.SolrServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/3/31.
 */
public class CreateSolrCollectionTest {
    CreateSolrCollection createSolrCollection;
    @Before
    public void setUp() throws Exception {
        createSolrCollection=new CreateSolrCollection("testCollection","test",2);
    }

    @After
    public void tearDown() throws Exception {
        SolrServer.getInstance().close();
    }

    @Test
    public void execute() throws Exception {
        createSolrCollection.execute();
    }

    @Test
    public void recovery() throws Exception {
        createSolrCollection.recovery();
    }

}