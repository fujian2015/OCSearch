package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.CommonUtils;
import com.asiainfo.ocsearch.core.TableSchema;
import com.asiainfo.ocsearch.db.solr.SolrServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/3/31.
 */
public class UploadConfigTest {

    private GenerateSolrConfig generateSolrConfig;

    private UploadConfig upLoadCOnfig;


    @Before
    public void setUp() throws Exception {
        TableSchema tableSchema = new TableSchema(CommonUtils.getRquestDemo());

        this.upLoadCOnfig = new UploadConfig(tableSchema.name);

        this.generateSolrConfig = new GenerateSolrConfig(tableSchema);
    }

    @After
    public void tearDown() throws Exception {
        this.generateSolrConfig.recovery();
        SolrServer.getInstance().close();
    }

    @Test
    public void execute() throws Exception {
        this.generateSolrConfig.execute();
        upLoadCOnfig.execute();
    }

    @Test
    public void recovery() throws Exception {
        upLoadCOnfig.recovery();
    }

}