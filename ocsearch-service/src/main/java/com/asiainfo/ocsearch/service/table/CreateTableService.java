package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.SchemaManager;
import com.asiainfo.ocsearch.meta.Table;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.TransactionUtil;
import com.asiainfo.ocsearch.transaction.atomic.*;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Created by mac on 2017/5/2.
 */
public class CreateTableService extends OCSearchService {

    private static Lock lock = new ReentrantLock();

    Logger log = Logger.getLogger(getClass());

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

        try {
            lock.lock();

            Table table = parseRequest(request);

            Schema schema = SchemaManager.getSchema(table.getSchema());
            if (schema == null) {
                throw new ServiceException("schema : " + table.getSchema() + " does not exist!", ErrorCode.PARSE_ERROR);
            }
            String name = table.getName();

            Transaction transaction = new TransactionImpl();

            Collection<Field> fields = schema.getFields().values();

            Set<String> families = fields.stream().map(field -> field.getHbaseFamily()).collect(Collectors.toSet());

            transaction.add(new CreateHbaseTable(name, table.getHbaseRegions(), table.getRegionSplits(), families));

            Table.IndexType indexType = table.getIndexType();

            if (indexType != Table.IndexType.HBASE_ONLY) {
                if (!ConfigUtil.configExists(schema.getName())) {
                    transaction.add(new GenerateSolrConfig(schema));
                    transaction.add(new UploadConfig(schema.getName()));
                    transaction.add(new GenerateIndxerConfig(schema));
                }
                transaction.add(new CreateSolrCollection(name, schema.getName(), table.getSolrShards(), table.getSolrReplicas()));

                if (indexType == Table.IndexType.HBASE_SOLR_INDEXER) {
                    transaction.add(new CreateIndexerTable(name, schema.getName()));
                }
            }

            transaction.add(new SaveTableToDb(table));

            if (transaction.canExecute()) {
                try {
                    String transactionName = uuid + "_add_" + name;

                    TransactionUtil.serialize(transactionName, transaction);
                    transaction.execute();
                    TransactionUtil.deleteTransaction(transactionName);
                } catch (RuntimeException e) {
                    try {
                        transaction.rollBack();
                    } catch (Exception rollBackException) {
                        log.error("roll back create table " + name + " failure", rollBackException);
                    }
                    throw e;
                }
            } else {
                throw new ServiceException(String.format("create table :%s  failure because of transaction can't be executed.", name), ErrorCode.RUNTIME_ERROR);
            }
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
        return success;
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

            int indexType = request.get("index_type").asInt();

            return new Table(name, schema, indexType, solrShards, solrReplicas, regions, regionsSplits);

        } catch (Exception e) {
            throw new ServiceException("parse error!", ErrorCode.PARSE_ERROR);
        }
    }
}
