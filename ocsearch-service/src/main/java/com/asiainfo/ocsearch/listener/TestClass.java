package com.asiainfo.ocsearch.listener;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.datasource.mysql.DataSourceProvider;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.service.table.CreateTableService;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.transaction.atomic.schema.CreateIndxerConfig;
import com.asiainfo.ocsearch.transaction.atomic.table.CreateIndexerTable;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Properties;

/**
 * Created by mac on 2017/5/8.
 */
public class TestClass {
    public static void main(String[] args) throws Exception {
        Properties properties = PropertiesLoadUtil.loadProFile("ocsearch.properties");

        DataSourceProvider.setUp(properties);

        new SystemListener().initSchemaManager();
        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "    \"name\": \"GPRS__20170510\",\n" +
                "    \"schema\": \"testSchema9\",\n" +
                "    \"hbase\": {\n" +
                "        \"region_num\": 100,\n" +
                "        \"region_split\": [\n" +
                "            \n" +
                "        ]\n" +
                "    },\n" +
                "    \"solr\": {\n" +
                "        \"shards\": 2,\n" +
                "        \"replicas\": 2\n" +
                "    }\n" +
                "}");
//        HbaseServiceManager.setup("hbase-site.xml");
        new CreateTableService().doService(jsonNode);
    }

    public static void testIndexer(JsonNode jsonNode) throws Exception {

        Properties properties = PropertiesLoadUtil.loadProFile("ocsearch.properties");

        IndexerServiceManager.setUp(properties);
        AtomicOperation atomicOperation= new CreateIndxerConfig(new Schema(jsonNode));

        if(atomicOperation.canExecute()){
            atomicOperation.execute();
        }

        AtomicOperation create= new CreateIndexerTable("table1","testSchema");

        if(create.canExecute()){
            System.out.println("execute...");
            create.execute();
        }
        else{
            System.out.println("recovery...");
            create.recovery();
        }

    }

}
