package com.asiainfo.ocsearch.service.internal;

import com.asiainfo.ocsearch.CommonUtils;
import com.asiainfo.ocsearch.core.TableSchema;
import com.asiainfo.ocsearch.transaction.internal.GenerateSolrConfig;
import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/3/28.
 */
public class CreateTableServiceTest {

    JsonNode request;
    @Before
    public void setUp() throws Exception {
        request = CommonUtils.getRquestDemo();
    }

    @After
    public void tearDown() throws Exception {
        TableSchema tableSchema = new TableSchema(request);
        new GenerateSolrConfig(tableSchema).recovery();
    }

    @Test
    public void doService() throws Exception {

        CreateTableService createTableService=new CreateTableService();

        createTableService.doService(request);
    }

}