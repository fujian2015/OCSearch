package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.cache.CacheManager;
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
import org.codehaus.jackson.map.ObjectMapper;
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
    protected JsonNode query(JsonNode request) throws ServiceException {

        ObjectNode returnData = JsonNodeFactory.instance.objectNode();
        int total;
        try {
            String qs = request.get("query").asText();
            String condition = request.get("condition").asText();

            int start = request.get("start").asInt();
            int rows = request.get("rows").asInt();
            String sort = request.get("sort").asText();

            ArrayNode tables = (ArrayNode) request.get("tables");
            Set<String> tableSet = new TreeSet<>();
            tables.forEach(table -> tableSet.add(table.asText()));

            String cacheKey = generateCacheKey(qs, condition, tableSet);

            String cacheStartKey = start + "|" + rows;
            Map<String, String> cacheValue;
            try {
                cacheValue = CacheManager.getCache().get(cacheKey, Arrays.asList("total", cacheStartKey));
            } catch (Exception e) {
                log.error("get cache error,called by:", e);
                cacheValue = new HashMap<>();
            }

            List<OCRowKey> rowKeys = new ArrayList<>(rows);

            final Map<String, List<String>> rowKeyMap = new HashMap<>();
            String tableFirst = tableSet.iterator().next();
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(tableFirst);

            if (cacheValue.containsKey("total") && cacheValue.containsKey(cacheStartKey)) {
                //get ids from cache
                ArrayNode keyNode = (ArrayNode) new ObjectMapper().readTree(cacheValue.get(cacheStartKey));
                keyNode.forEach(node -> {
                    String rs[] = StringUtils.split(node.asText(), "||");
                    rowKeys.add(new OCRowKey(rs[0], rs[1]));
                    if (!rowKeyMap.containsKey(rs[0]))
                        rowKeyMap.put(rs[0], new ArrayList<>());
                    rowKeyMap.get(rs[0]).add(rs[1]);
                });
                total = Integer.parseInt(cacheValue.get("total"));
            } else {
                //get ids from solr
                SolrQuery solrQuery = constructQuery(start, rows, qs, condition, sort, tableSet, schema.getQueryFields());
                System.err.println("solr query is:" + solrQuery.toString());
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
            }

            Set<String> returnFields = generateReturnFields(schema, (ArrayNode) request.get("return_fields"));

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
                for (String table : rowKeyMap.keySet()) {

                    HbaseQuery hbaseQuery = new HbaseQuery(schema, table, returnFields, rowKeyMap.get(table));

                    QueryActor queryActor = new GetQueryActor(hbaseQuery, runningThreadNum);

                    ThreadPoolManager.getExecutor("getQuery").submit(queryActor);

                    actors.put(table, queryActor);
                }
                runningThreadNum.await();
                for (OCRowKey rowKey : rowKeys) {
                    ObjectNode data = actors.get(rowKey.table).getQueryResult().getData().remove(0);
                    if (data.get("id") == null) {
                        data.put("id", rowKey.rowKey);
                    }
                    data.put("_table_", rowKey.table);
                    arrayNode.add(data);
                }
            }

            //cache put
            Map<String, String> caches = new HashMap<>();
            if (!cacheValue.containsKey("total")) {
                caches.put("total", String.valueOf(total));
            }
            if (!cacheValue.containsKey(cacheStartKey))
                caches.put(cacheStartKey, generateCacheValue(rowKeys));

            if (!caches.isEmpty())
                CacheManager.getCache().put(cacheKey, caches);
            //return data
            returnData.put("total", total);
            returnData.put("docs", arrayNode);
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
        return returnData;
    }

    private String generateCacheValue(List<OCRowKey> rowKeys) {
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
