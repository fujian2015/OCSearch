package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.Table;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.table.*;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mac on 2017/5/2.
 */
public class CreateTableService extends OCSearchService {

    private static Lock lock = new ReentrantLock();

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

        try {
            lock.lock();

            Table table = parseRequest(request);

//            Schema schema = SchemaManager.getSchemaBySchema(table.getSchema());
            Schema schema = MetaDataHelperManager.getInstance().getSchemaBySchema(table.getSchema());

            if (schema == null) {
                throw new ServiceException("schema : " + table.getSchema() + " does not exist!", ErrorCode.PARSE_ERROR);
            }
            String name = table.getName();

            Transaction transaction = new TransactionImpl();

            Set<String> families = new HashSet<>();

            schema.getFields().values().stream()
                    .filter(field -> field.getInnerField() == null)
                    .forEach(field -> families.add(field.getHbaseFamily()));
            schema.getInnerFields().values().stream().forEach(innerField -> families.add(innerField.getHbaseFamily()));

            if (!request.get("hbase").has("exist")) //hbase table does not exist
                transaction.add(new CreateHbaseTable(name, table.getHbaseRegions(), table.getRegionSplits(), families));

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER || indexType == IndexType.HBASE_SOLR_BATCH) {

                transaction.add(new CreateSolrCollection(name, schema.getName(), table.getSolrShards(), table.getSolrReplicas()));

                transaction.add(new CreateIndexerTable(name, schema));

            } else if (indexType == IndexType.PHOENIX) {
                transaction.add(new CreatePhoenixView(name, schema));
            }

//            transaction.add(new SaveTableToDb(table));

//            transaction.add(new SaveTableToMemory(name, schema.getName()));
            transaction.add(new SaveTableToZk(table)); //instead db with zookeeper

            if (!transaction.canExecute()) {
                throw new ServiceException(String.format("create table :%s  failure because of transaction can't be executed.", name), ErrorCode.RUNTIME_ERROR);
            }
            try {
                transaction.execute();
            } catch (Exception e) {
                log.error(e);
                try {
                    transaction.rollBack();
                } catch (Exception rollBackException) {
                    TransactionUtil.serialize(uuid + "_table_create_" + name, transaction, true);
                    log.error("roll back create table " + name + " failure", rollBackException);
                }
                throw e;
            }
            return success;
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            lock.unlock();
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }

    }

    /**
     * /**
     * {
     * "name": "GPRS__20140927",
     * "schema": "testSchema",
     * "hbase": {
     * "region_num": 100,
     * "region_split": [
     * "a",
     * "b",
     * "c"
     * ]
     * },
     * "solr": {
     * "shards": 10,
     * "replicas": 2
     * },
     * "index_type": 0
     * }
     *
     * @param request
     * @return
     */
    private Table parseRequest(JsonNode request) throws ServiceException {

        try {

            String name = request.get("name").asText();

            String schema = request.get("schema").asText();

            JsonNode hbaseNode = request.get("hbase");

            int regions = -1;

            if (hbaseNode.get("region_num") != null)
                regions = hbaseNode.get("region_num").getIntValue();

            TreeSet<String> regionsSplits = new TreeSet<>();

            if (hbaseNode.get("region_split") != null) {
                ArrayNode regionList = (ArrayNode) hbaseNode.get("region_split");
                regionList.forEach(jsonNode -> regionsSplits.add(jsonNode.asText()));
            }

            JsonNode solrNode = request.get("solr");

            int solrShards = solrNode.get("shards").asInt();

            int solrReplicas = -1;

            if (solrNode.get("replicas") != null) {
                solrReplicas = solrNode.get("replicas").asInt();
            }

            return new Table(name, schema, solrShards, solrReplicas, regions, regionsSplits);

        } catch (Exception e) {
            throw new ServiceException("parse error!", ErrorCode.PARSE_ERROR);
        }
    }
}
