package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.query.*;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.google.common.collect.Sets;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/31.
 */
public class FileGetService extends OCSearchService {

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {

        try {
            if (false == request.has("id")) {
                throw new ServiceException("the get service request must have 'id' param keys!", ErrorCode.PARSE_ERROR);
            }
            String oriId = request.get("id").asText();

            FileID fileID = FileID.parseId(oriId);

            String table = fileID.getTable();

            String field=fileID.getField();
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(table);

            if (schema == null)
                throw new ServiceException("the table does not exist!", ErrorCode.TABLE_NOT_EXIST);

            HbaseQuery hbaseQuery = new HbaseQuery(schema, table, Sets.newHashSet(field), Arrays.asList(fileID.getRowKey()));

            CountDownLatch runningThreadNum = new CountDownLatch(1);

            QueryActor queryActor = new GetQueryActor(hbaseQuery, runningThreadNum);

            ThreadPoolManager.getExecutor("getQuery").submit(queryActor);

            runningThreadNum.await();

            QueryResult result = queryActor.getQueryResult();
            if (null != result.getLastError()) {
                throw result.getLastError();
            } else {

                ObjectNode node = result.getData().remove(0);
                if(node.has(field))
                    return node.get(field).getBinaryValue();
                throw new ServiceException("the id is not exists!",ErrorCode.FILE_NOT_EXISTS);
            }
        } catch (IOException e) {
            log.warn(e);
            throw new ServiceException("the request id is invalid !", ErrorCode.PARSE_ERROR);
        } catch (InterruptedException e) {
            log.error(e);
            throw new ServiceException( e,ErrorCode.RUNTIME_ERROR);
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException( e,ErrorCode.RUNTIME_ERROR);
        }
    }
}
