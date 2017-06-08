package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/6/2.
 */
public class BatchIndexService extends OCSearchService{
    Logger stateLog = Logger.getLogger("state");
    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();

        try {
            stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

            String table = request.get("table").asText();

            long startTime=request.has("start_time")?request.get("start_time").asLong():-1;
            long endTime=request.has("end_time")?request.get("end_time").asLong():-1;

//            ExecutorService executor = ThreadPoolManager.getExecutor("scanQuery");
//            executor.submit(new Runnable() {
//                @Override
//                public void run() {
//                    int res = IndexerServiceManager.getIndexerService().batchIndex(table,startTime,endTime);
//                    System.err.println(res);
//                    if(res!=0)
//                        log.error("execute batch index error!");
//                    else
//                        log.info("execute batch index success!");
//                }
//            });
            int res = IndexerServiceManager.getIndexerService().batchIndex(table,startTime,endTime);
            log.info("return code:"+res);
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }
        return success;
    }
}
