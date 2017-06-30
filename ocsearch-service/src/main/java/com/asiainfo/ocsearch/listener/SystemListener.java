package com.asiainfo.ocsearch.listener;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.datasource.jdbc.pool.DbPool;
import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.meta.*;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.scheduler.ProcessTransaction;
import com.asiainfo.ocsearch.scheduler.RollBackTranscation;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by mac on 2017/4/28.
 */
public class SystemListener implements ServletContextListener {

    Logger logger = Logger.getLogger(getClass());

    ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);


    @Deprecated
    public void initSchemaManager() throws IOException, SQLException {

        MyBaseService myBaseService = MyBaseService.getInstance();
        String querySchema = "select * from  schema_def ";

        try {
            List<Map<String, Object>> results = myBaseService.queryList(querySchema);

            for (Map<String, Object> result : results) {
                String name = String.valueOf(result.get("name"));
                String tableExpression = String.valueOf(result.get("table_expression"));
                String rowkeyExpression = String.valueOf(result.get("rowkey_expression"));

                int indexType = (int) result.get("index_type");

                Schema schema = new Schema(name, tableExpression, rowkeyExpression, IndexType.valueOf(indexType));

                ObjectMapper objectMapper = new ObjectMapper();

                String contentField = String.valueOf(result.get("content_field"));
//                if (StringUtils.isNotEmpty(contentField)) {
//                    schema.setContentField(new ContentField(objectMapper.readTree(contentField)));
//                }

                String queryFieldsString = String.valueOf(result.get("query_fields"));

                if (StringUtils.isNotEmpty(queryFieldsString)) {

                    List<QueryField> queryFields = new ArrayList<>();
                    ArrayNode queryFieldsNode = (ArrayNode) objectMapper.readTree(queryFieldsString);
                    for (JsonNode node : queryFieldsNode) {
                        queryFields.add(new QueryField(node));
                    }
                    schema.setQueryFields(queryFields);
                }
                schema.setFields(getFields(name, myBaseService));

                SchemaManager.addSchema(name, schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("init schema manager error!", e);
            throw e;
        }

        String queryTable = "select `name`,`schema_name` from  table_def ";

        try {
            List<Map<String, Object>> results = myBaseService.queryList(queryTable);

            for (Map<String, Object> result : results) {

                String name = String.valueOf(result.get("name"));
                String schema = String.valueOf(result.get("schema_name"));

                SchemaManager.addTable(name, schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("init schema manager error!", e);
            throw e;
        }

    }

    @Deprecated
    private Map<String, Field> getFields(String schema, MyBaseService myBaseService) throws SQLException {

        String queryField = "select * from  field_def where `schema_name` = '" + schema + "'";

        List<Map<String, Object>> fieldResult = myBaseService.queryList(queryField);

        Map<String, Field> fields = new HashMap<>();

        for (Map<String, Object> map : fieldResult) {
            String name = String.valueOf(map.get("name"));
            boolean indexed = Boolean.valueOf((String) map.get("indexed"));
            boolean indexContented = Boolean.valueOf((String) map.get("index_contented"));
            boolean indexStored = Boolean.valueOf((String) map.get("index_stored"));
            String indexType = String.valueOf(map.get("index_type"));
            String hbaseColumn = String.valueOf(map.get("hbase_column"));
            String hbaseFamily = String.valueOf(map.get("hbase_family"));
            String storeType = String.valueOf(map.get("store_type"));
//            fields.put(name, new Field(name, indexed, indexContented, indexStored, indexType, storeType, hbaseColumn, hbaseFamily));
        }
        return fields;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        logger.info("start ocsearch begin...");

        try {
            initAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("start ocsearch  end.");

    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        logger.info("begin destroy ocsearch...");
        scheduledThreadPool.shutdownNow();
        try {
            scheduledThreadPool.awaitTermination(5, TimeUnit.MINUTES);

            if (IndexerServiceManager.getIndexerService() != null) {
                IndexerServiceManager.getIndexerService().close();
            }
            if (HbaseServiceManager.getInstance() != null) {
                HbaseServiceManager.getInstance().close();
            }
            if (SolrServerManager.getInstance() != null) {
                SolrServerManager.getInstance().close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);
        }
        logger.info("end destroy ocsearch...");
        System.exit(0);
    }


    public void initAll() throws Exception {

        logger.info("begin init ocsearch...");

        Properties properties = PropertiesLoadUtil.loadProFile("ocsearch.properties");

        OCSearchEnv.setUp(properties);

        ThreadPoolManager.setUp();

        HbaseServiceManager.setup("hbase-site.xml");
        MetaDataHelperManager.setUp(properties);

        boolean hbaseOnly = Boolean.valueOf(OCSearchEnv.getEnvValue("HBASE_ONLY"));

        if (!hbaseOnly) {
            SolrServerManager.setUp(properties);
            IndexerServiceManager.setUp(properties);
        }
        //initial phoenix connection pool
        boolean usePhoenix = Boolean.valueOf(OCSearchEnv.getEnvValue("USE_PHOENIX"));
        if (usePhoenix) {
            Properties hbase = PropertiesLoadUtil.loadXmlFile("hbase-site.xml");
            Properties druid = PropertiesLoadUtil.loadProFile("druid.properties");
            DbPool.setUp(druid, hbase);
        }

        int processPeriod = Integer.parseInt(OCSearchEnv.getEnvValue("TRANSACTION_PROCESS_PERIOD", "10"));
        scheduledThreadPool.scheduleAtFixedRate(new ProcessTransaction(), 0, processPeriod, TimeUnit.MINUTES);

        int rollbackPeriod = Integer.parseInt(OCSearchEnv.getEnvValue("TRANSACTION_ROLLBACK_PERIOD", "10"));
        scheduledThreadPool.scheduleAtFixedRate(new RollBackTranscation(), 0, rollbackPeriod, TimeUnit.MINUTES);

        logger.info("end init ocsearch...");

    }

}
