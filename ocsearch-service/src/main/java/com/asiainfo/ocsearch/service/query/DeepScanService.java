package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelper;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.query.HbaseQuery;
import com.asiainfo.ocsearch.query.QueryResult;
import com.asiainfo.ocsearch.query.ScanQueryActor;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Created by mac on 2017/6/29.
 */
public class DeepScanService extends QueryService {
    @Override
    protected JsonNode query(JsonNode request) throws ServiceException {

        ObjectNode returnData = JsonNodeFactory.instance.objectNode();
        ArrayNode results = JsonNodeFactory.instance.arrayNode();
        int total = 0;
        try {
            if (false == (request.has("rowkey_prefix") && request.has("cursor_mark") && request.has("rows") && request.has("tables"))) {
                throw new ServiceException("the search service request must have 'rowkey_prefix','cursor_mark'," +
                        "'rows','tables' param keys!", ErrorCode.PARSE_ERROR);
            }

            String rowKey = request.get("rowkey_prefix").asText();

            String cursorMark = request.get("cursor_mark").asText();
            String condition = null;

            if (request.has("condition"))
                condition = request.get("condition").asText();

            int rows = request.get("rows").asInt();

            ArrayNode tables = (ArrayNode) request.get("tables");

            if (tables.size() > 1)
                throw new ServiceException("the search service request must have only one 'table'", ErrorCode.PARSE_ERROR);


            ArrayNode returnNode = (ArrayNode) request.get("return_fields");

            MetaDataHelper metaDataHelper = MetaDataHelperManager.getInstance();

            String table = tables.get(0).asText();

            assertTables(Sets.newHashSet(table));

            Schema schema = metaDataHelper.getSchemaByTable(table);
            Set<String> returnFields = generateReturnFields(schema, returnNode);

            String stopKey = getStopKey(rowKey);
            String startKey = rowKey;

            if (false == StringUtils.equals("*", cursorMark))
                startKey = cursorMark;

            CountDownLatch countDownLatch = new CountDownLatch(1);

            ExecutorService executor = ThreadPoolManager.getExecutor("scanQuery");

            HbaseQuery hbaseQuery = new HbaseQuery(schema, table, startKey, stopKey, rows, condition, returnFields);
            hbaseQuery.setNeedTotal(false);

            ScanQueryActor scanQueryActor = new ScanQueryActor(hbaseQuery, countDownLatch);
            executor.submit(scanQueryActor);

            countDownLatch.await();

            QueryResult qr = scanQueryActor.getQueryResult();

            boolean withId = hasId(returnNode);
            for (ObjectNode data : qr.getData()) {
                if (withId == false)
                    data.remove("id");
                results.add(data);
            }

            //return data
            returnData.put("next_cursor_mark", qr.getLastRowkey() == null ? "" : qr.getLastRowkey());

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
