package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import com.asiainfo.ocsearch.meta.QueryField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.query.GetQueryActor;
import com.asiainfo.ocsearch.query.HbaseQuery;
import com.asiainfo.ocsearch.query.QueryActor;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/23.
 */
public class SearchService extends QueryService {
    @Override
    public JsonNode query(JsonNode request) throws ServiceException {

        ObjectNode returnData = JsonNodeFactory.instance.objectNode();

        try {
            if (false == (request.has("query") && request.has("condition") && request.has("start")
                    && request.has("rows") && request.has("sort") && request.has("tables"))) {
                throw new ServiceException("the search service request must have 'query','condition','start'," +
                        "'rows','sort','tables' param keys!", ErrorCode.PARSE_ERROR);
            }

            String qs = request.get("query").asText();
            String condition = request.get("condition").asText();

            int start = request.get("start").asInt();
            int rows = request.get("rows").asInt();
            String sort = request.get("sort").asText();
            ArrayNode returnNode = (ArrayNode) request.get("return_fields");
            ArrayNode tables = (ArrayNode) request.get("tables");
            Set<String> tableSet = new TreeSet<>();
            tables.forEach(table -> tableSet.add(table.asText()));

            assertTables(tableSet);

            if (tableSet.size() == 1) {
                searchSingleTable(qs, condition, start, rows, sort, tableSet, returnNode, returnData);
            } else {
                searchMultiTable(qs, condition, start, rows, sort, tableSet, returnNode, returnData);
            }

        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
        return returnData;
    }

    private void searchMultiTable(String qs, String condition, int start, int rows, String sort, Set<String> tableSet, ArrayNode returnNode, ObjectNode returnData) throws Exception {
        int total = 0;

        List<OCRowKey> rowKeys = new ArrayList<>(rows);

        final Map<String, List<String>> rowKeyMap = new HashMap<>();
        String tableFirst = tableSet.iterator().next();
        Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(tableFirst);

        //get ids from solr
        SolrQuery solrQuery = constructQuery(start, rows, qs, condition, sort, tableSet, schema.getQueryFields());
        log.info("solr query is:" + solrQuery.toString());
        SolrDocumentList solrResults = SolrServerManager.getInstance().query(tableFirst, solrQuery);

        total = (int) solrResults.getNumFound();

        solrResults.forEach(doc -> {
            String table = (String) doc.get("_table_");
            String id = (String) doc.get("id");
            if (!rowKeyMap.containsKey(table))
                rowKeyMap.put(table, new ArrayList<>());
            rowKeyMap.get(table).add(id);
            rowKeys.add(new OCRowKey(table, id));
        });


        Set<String> returnFields = generateReturnFields(schema, returnNode);

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

        if (returnFields.isEmpty()) {
            //need id only
            rowKeys.forEach(ocRowKey -> {
                ObjectNode doc = JsonNodeFactory.instance.objectNode();
                doc.put("id", ocRowKey.rowKey);
                doc.put("_table_", ocRowKey.table);
                arrayNode.add(doc);
            });
        } else {
            //read data from hbase
            CountDownLatch runningThreadNum = new CountDownLatch(rowKeyMap.keySet().size());

            Map<String, QueryActor> actors = new HashMap<>();
            rowKeyMap.entrySet().forEach(entry -> {
                String table = entry.getKey();

                HbaseQuery hbaseQuery = new HbaseQuery(schema, table, returnFields, entry.getValue());

                QueryActor queryActor = new GetQueryActor(hbaseQuery, runningThreadNum);

                ThreadPoolManager.getExecutor("getQuery").submit(queryActor);

                actors.put(table, queryActor);
            });
            runningThreadNum.await();
            boolean withTable = hasTable(returnNode);
            boolean withId = hasId(returnNode);
            for (OCRowKey rowKey : rowKeys) {
                ObjectNode data = actors.get(rowKey.table).getQueryResult().getData().remove(0);

                arrayNode.add(updateNode(data,rowKey.rowKey,rowKey.table,withTable,withId));
            }
        }
        //return data
        returnData.put("total", total);
        returnData.put("docs", arrayNode);
    }


    private void searchSingleTable(String qs, String condition, int start, int rows, String sort, Set<String> tableSet, ArrayNode returnNode, ObjectNode returnData) throws Exception {
        int total = 0;
        long startTime = System.currentTimeMillis();

        List<String> rowKeys = new ArrayList<>(rows);

        String table = tableSet.iterator().next();
        Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(table);

        //get ids from solr
        SolrQuery solrQuery = constructQuery(start, rows, qs, condition, sort, tableSet, schema.getQueryFields());

        log.info("solr query is:" + solrQuery.toString());

        SolrDocumentList solrResults = SolrServerManager.getInstance().query(table, solrQuery);

        total = (int) solrResults.getNumFound();

        solrResults.forEach(doc -> {
            rowKeys.add((String) doc.get("id"));
        });

        long getIdsTime = System.currentTimeMillis();

        log.info("[ocsearch]get ids use :" + (getIdsTime - startTime) + "ms");

        Set<String> returnFields = generateReturnFields(schema, returnNode);

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        if (returnFields.isEmpty()) {
            //need id only
            rowKeys.forEach(id -> {
                ObjectNode doc = JsonNodeFactory.instance.objectNode();
                doc.put("id", id);
                arrayNode.add(doc);
            });
        } else {
            //read data from hbase
            CountDownLatch runningThreadNum = new CountDownLatch(1);
            HbaseQuery hbaseQuery = new HbaseQuery(schema, table, returnFields, rowKeys);
            QueryActor queryActor = new GetQueryActor(hbaseQuery, runningThreadNum);
            ThreadPoolManager.getExecutor("getQuery").submit(queryActor);
            runningThreadNum.await();
            boolean withId = hasId(returnNode);
            boolean withTable = hasTable(returnNode);
            log.warn("result is:" + queryActor.getQueryResult().getData().size());
            rowKeys.forEach(row -> {
                        ObjectNode data = queryActor.getQueryResult().getData().remove(0);

                        arrayNode.add(updateNode(data,row,table,withTable,withId));
                    }
            );
            log.warn("arrayNode  is:" + arrayNode.size());
        }
        long getDataTime = System.currentTimeMillis();

        log.info("[ocsearch]get hbase data use :" + (getDataTime - getIdsTime) + "ms");

        //return data
        returnData.put("total", total);
        arrayNode.toString();
        returnData.put("docs", arrayNode);

    }

    private String generateSingleCacheValue(List<String> rows) {
        ArrayNode node = JsonNodeFactory.instance.arrayNode();
        rows.forEach(ocRowKey -> node.add(ocRowKey));
        return node.toString();
    }

    private String generateMultiCacheValue(List<OCRowKey> rowKeys) {
        ArrayNode node = JsonNodeFactory.instance.arrayNode();
        rowKeys.forEach(ocRowKey -> node.add(ocRowKey.table + "||" + ocRowKey.rowKey));
        return node.toString();
    }


    private SolrQuery constructQuery(int start, int rows, String qs, String condition, String sort, Set<String> tables, List<QueryField> queryFields) throws ServiceException {

        StringBuilder q = new StringBuilder();


        SolrQuery solrQuery = new SolrQuery(q.toString());
        solrQuery.setRows(rows);
        solrQuery.setStart(start);

        if (StringUtils.isNotBlank(qs)) {

            solrQuery.setQuery(qs);
            updateDisMax(solrQuery, queryFields);

            if (StringUtils.isNotBlank(condition))
                solrQuery.setFilterQueries(condition);
        } else if (StringUtils.isNotBlank(condition)) {
            solrQuery.setQuery(condition);
        } else {
            solrQuery.setQuery("*:*");
        }

        if (tables.size() > 1) { //如果tables多的话取
            solrQuery.set("collection", StringUtils.join(tables, ","));
            solrQuery.set("fl", "id,_table_");
        } else {
            solrQuery.set("fl", "id");
        }

        if (StringUtils.isBlank(sort))
            return solrQuery;

        String[] sorts = sort.split(",");
        for (String s : sorts) {
            int index;
            if ((index = s.indexOf("asc")) != -1) {
                solrQuery.addSort(s.substring(0, index).trim(), SolrQuery.ORDER.asc);
            } else if ((index = s.indexOf("desc")) != -1) {
                solrQuery.addSort(s.substring(0, index).trim(), SolrQuery.ORDER.desc);
            } else {
                throw new ServiceException("sort string must contains  'asc' or 'desc'", ErrorCode.PARSE_ERROR);
            }
        }
        return solrQuery;
    }

    private void updateDisMax(SolrQuery solrQuery, List<QueryField> queryFields) {
        solrQuery.set("defType", "dismax");
        Set<String> names = new TreeSet<>();
        Set<String> qfs = new TreeSet<>();
        queryFields.forEach(qf -> {
            String name = qf.getName();
            int weight = qf.getWeight();
            names.add(name);
            qfs.add(name + "^" + weight);
        });

        solrQuery.set("qf", StringUtils.join(names, " "));
        solrQuery.set("pf", StringUtils.join(qfs, " "));
    }

}
