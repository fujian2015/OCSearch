package com.asiainfo.ocsearch.listener;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
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

//        SolrServerManager.setUp(properties);
//        DataSourceProvider.setUp(properties);

//        new SystemListener().initAll();
        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "\t\"request\":true,\n" +
                "    \"name\": \"testSchema10\",\n" +
                "    \"rowkey_expression\": \"md5(phone,imsi)+‘|‘+phone+‘|‘+imsi\",\n" +
                "    \"table_expression\": \"table+’_'+time\",\n" +
                "    \"index_type\": 0,\n" +
                "    \"content_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"_root_\",\n" +
                "            \"type\": \"text_gl\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"inner_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"basic\",\n" +
                "            \"separator\": \";\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"query_fields\": [\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"weight\": 10\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"weight\": 20\n" +
                "        }\n" +
                "    ],\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"length\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"int\",\n" +
                "            \"store_type\": \"INT\",\n" +
                "            \"content_field\": \"_root_\",\n" +
                "            \"inner_field\": \"basic\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"title\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": true,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\",\n" +
                "            \"content_field\": \"_root_\",\n" +
                "            \"inner_field\": \"basic\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"content\",\n" +
                "            \"indexed\": true,\n" +
                "            \"index_stored\": false,\n" +
                "            \"index_type\": \"text_gl\",\n" +
                "            \"store_type\": \"STRING\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");

        Schema schema = new Schema(jsonNode);


        String tableString = "{\n" +
                "    \"name\": \"GPRS__20170510\",\n" +
                "    \"schema\": \"testSchema10\",\n" +
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
                "}";
System.out.println(jsonNode);
//        new CreateTableService().doService(new ObjectMapper().readTree(tableString));
//        new DeleteTableService().doService(new ObjectMapper().readTree(tableString));
//        new CreateIndexerTable("GPRS__20170510",schema).execute();
//        Configuration conf = new Configuration();
//        conf.addResource("hbase-site.xml");
//        ReplicationAdmin replicationAdmin = new ReplicationAdmin(HBaseConfiguration.create(conf));
//        replicationAdmin.disablePeer("Indexer_GPRS__20170510");
//        replicationAdmin.removePeer("Indexer_GPRS__20170510");

    }

    public static void testIndexer(JsonNode jsonNode) throws Exception {

        Properties properties = PropertiesLoadUtil.loadProFile("ocsearch.properties");

        IndexerServiceManager.setUp(properties);


        AtomicOperation create = new CreateIndexerTable("table1", new Schema(jsonNode));

        if (create.canExecute()) {
            System.out.println("execute...");
            create.execute();
        } else {
            System.out.println("recovery...");
            create.recovery();
        }

    }



}
