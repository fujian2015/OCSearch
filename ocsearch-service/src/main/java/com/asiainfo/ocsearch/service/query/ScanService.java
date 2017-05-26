package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.cache.CacheManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelper;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.query.HbaseQuery;
import com.asiainfo.ocsearch.query.QueryActor;
import com.asiainfo.ocsearch.query.QueryResult;
import com.asiainfo.ocsearch.query.ScanQueryActor;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Created by mac on 2017/5/19.
 */
public class ScanService extends QueryService {
    @Override
    protected JsonNode query(JsonNode request) throws ServiceException {

        ObjectNode returnData = JsonNodeFactory.instance.objectNode();
        ArrayNode results = JsonNodeFactory.instance.arrayNode();
        int total = 0;
        try {
            String rowKey = request.get("rowkey_prefix").asText();
            String condition = request.get("condition").asText();

            int start = request.get("start").asInt();
            int rows = request.get("rows").asInt();

            ArrayNode tables = (ArrayNode) request.get("tables");
            Set<String> tableSet = new TreeSet<>();
            tables.forEach(table -> tableSet.add(table.asText()));

            String cacheKey = generateCacheKey(rowKey, condition, tableSet);
            ArrayNode returnNode = (ArrayNode) request.get("return_fields");

            Map<String, String> cacheValue = null;
            try {
                cacheValue = CacheManager.getCache().get(cacheKey, Arrays.asList("total", start + ""));
            } catch (Exception e) {
                log.error("get cache error,called by:", e);
                cacheValue = new HashMap<>();
            }
            MetaDataHelper metaDataHelper = MetaDataHelperManager.getInstance();

            Schema schema = null;
            Set<String> returnFields = null;
            for (String table : tableSet) {
                schema = metaDataHelper.getSchemaByTable(table);
                returnFields = generateReturnFields(schema, returnNode);
                break;
            }
            ObjectNode totalNode = null;
            String nextRowKey = null;
            String firstKey = null;
            if (!cacheValue.containsKey("total") && start == 0)//never query
            {
                String startKey = rowKey;
                String stopKey = getStopKey(startKey);
                CountDownLatch countDownLatch = new CountDownLatch(tableSet.size());

                ExecutorService executor = ThreadPoolManager.getExecutor("scanQuery");

                Map<String, ScanQueryActor> actors = new TreeMap<>();
                for (String table : tableSet) {

                    HbaseQuery hbaseQuery = new HbaseQuery(schema, table, startKey, stopKey, rows, condition, returnFields);
                    hbaseQuery.setNeedTotal(true);
                    ScanQueryActor scanQueryActor = new ScanQueryActor(hbaseQuery, countDownLatch);
                    executor.submit(scanQueryActor);
                    actors.put(table, scanQueryActor);
                }

                countDownLatch.await();
                totalNode = JsonNodeFactory.instance.objectNode();
                for (String table : actors.keySet()) {
                    QueryResult qr = actors.get(table).getQueryResult();
                    totalNode.put(table, qr.getTotal());
                    total += qr.getTotal();
                    for (ObjectNode data : qr.getData()) {
                        if (firstKey == null) {
                            firstKey = data.get("id").asText();
                        }
                        if (results.size() == rows) {
                            nextRowKey = data.get("id").asText();
                            break;
                        }
                        data.put("_table_", table);
                        results.add(data);
                    }
                    if (nextRowKey != null)
                        break;
                }
                totalNode.put("total", total);

            } else {
                String stopKey = getStopKey(rowKey);
                if (!cacheValue.containsKey("total")) {
                    String startKey = rowKey;

                    CountDownLatch countDownLatch = new CountDownLatch(tableSet.size());

                    ExecutorService executor = ThreadPoolManager.getExecutor("scanQuery");

                    Map<String, ScanQueryActor> actors = new TreeMap<>();
                    for (String table : tableSet) {

                        HbaseQuery hbaseQuery = new HbaseQuery(schema, table, startKey, stopKey, 0, condition, returnFields);
                        hbaseQuery.setNeedTotal(true);
                        ScanQueryActor scanQueryActor = new ScanQueryActor(hbaseQuery, countDownLatch);
                        executor.submit(scanQueryActor);
                        actors.put(table, scanQueryActor);
                    }

                    countDownLatch.await();

                    totalNode = JsonNodeFactory.instance.objectNode();

                    for (String table : actors.keySet()) {
                        QueryResult qr = actors.get(table).getQueryResult();
                        totalNode.put(table, qr.getTotal());
                        total += qr.getTotal();
                        for (ObjectNode data : qr.getData()) {
                            if (firstKey == null) {
                                firstKey = data.get("id").asText();
                            }
                        }
                        if (nextRowKey != null)
                            break;
                    }
                    totalNode.put("total", total);
                } else {
                    totalNode = (ObjectNode) new ObjectMapper().readTree(cacheValue.get("total"));
                }

                ////start scan
                total = totalNode.get("total").asInt();

                int offset = 0;
                if (total <= start) {
                    throw new ServiceException("start param is larger than total number!", ErrorCode.PARSE_ERROR);
                }
                boolean isFirst = true;
                List<HbaseQuery> queries = new ArrayList<>();
                for (String table : tableSet) {

                    int size = totalNode.get(table).asInt();

                    if (offset + size <= start) {
                        offset = offset + size;
                    } else if (isFirst) {
                        isFirst = false;
                        String startKey = null;
                        int skip = 0;
                        if (cacheValue.containsKey("" + start)) {
                            startKey = cacheValue.get("" + start);
                        } else {
                            startKey = rowKey;
                            skip = start - offset;
                        }
                        HbaseQuery hbaseQuery = new HbaseQuery(schema, table, startKey, stopKey, rows, condition, returnFields);
                        hbaseQuery.setSkip(skip);
                        queries.add(hbaseQuery);
                    } else if (offset + size <= start + rows) {

                        queries.add(new HbaseQuery(schema, table, rowKey, stopKey, rows, condition, returnFields));
                    } else {
                        int limit = start + rows - offset;
                        queries.add(new HbaseQuery(schema, table, rowKey, stopKey, limit, condition, returnFields));
                        break;
                    }
                    offset = offset + size;
                }

                CountDownLatch countDownLatch = new CountDownLatch(queries.size());

                ExecutorService executor = ThreadPoolManager.getExecutor("scanQuery");

                Map<String, QueryActor> actors = new TreeMap<>();
                queries.forEach(query -> {
                    QueryActor actor = new ScanQueryActor(query, countDownLatch);
                    executor.submit(actor);
                    actors.put(query.getTable(), actor);

                });
                countDownLatch.await();

                for (String table : actors.keySet()) {
                    QueryResult qr = actors.get(table).getQueryResult();

                    for (ObjectNode data : qr.getData()) {
                        if (firstKey == null)
                            firstKey = data.get("id").asText();
                        data.put("_table_", table);
                        results.add(data);
                    }
                    if (qr.getLastRowkey() != null) {
                        nextRowKey = qr.getLastRowkey();
                    }
                }
            }

            //cache put
            Map<String, String> caches = new HashMap<>();
            if (!cacheValue.containsKey("total")) {
                caches.put("total", totalNode.toString());
            }
            if (!cacheValue.containsKey("" + start))
                caches.put("" + start, firstKey);
            if (nextRowKey != null)
                caches.put("" + (start + rows), nextRowKey);
            if (!caches.isEmpty())
                CacheManager.getCache().put(cacheKey, caches);
            //return data
            returnData.put("total", total);
            returnData.put("docs", results);
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }

        return returnData;
    }

    private String getStopKey(String startKey) {

        byte[] stopKey = Bytes.toBytes(startKey);
        stopKey[stopKey.length - 1]++;
        return Bytes.toString(stopKey);
    }



}
