package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import java.util.concurrent.ExecutorService;

/**
 * Created by mac on 2017/6/2.
 */
public class BatchIndexService extends OCSearchService {
    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();

        try {
            stateLog.info("start batch index request " + uuid + " at " + System.currentTimeMillis());

            String table = request.get("table").asText();

            if (!MetaDataHelperManager.getInstance().hasTable(table)) {
                throw new ServiceException("table " + table + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(table);

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER || indexType == IndexType.HBASE_SOLR_PHOENIX) {
                if (!IndexerServiceManager.getIndexerService().exists(table))
                    throw new ServiceException(String.format("indexer %s does not exist!", table), ErrorCode.INDEXER_NOT_EXIST);
            } else {
                throw new ServiceException("indexer " + table + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }

            long beginTime = request.has("begin_time") ? request.get("begin_time").asLong() : -1;
            long endTime = request.has("end_time") ? request.get("end_time").asLong() : -1;

            ExecutorService executor = ThreadPoolManager.getExecutor("batchIndex");
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int res = IndexerServiceManager.getIndexerService().batchIndex(table, beginTime, endTime);
                        log.info("return code:" + res);
                        if (res != 0)
                            log.error("execute batch index error!");
                        else
                            log.info("execute batch index success!");
                    } catch (Exception e) {
                        log.error(e);
                    }
                }
            });
//            int res = IndexerServiceManager.getIndexerService().batchIndex(table,startTime,endTime);

        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            stateLog.info("end batch index request " + uuid + " at " + System.currentTimeMillis());
        }
        return success;
    }
}
