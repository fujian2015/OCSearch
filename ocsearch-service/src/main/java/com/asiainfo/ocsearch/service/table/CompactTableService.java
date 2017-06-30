package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/6/29.
 */
public class CompactTableService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {

        try {
            String name = request.get("name").asText();

            if (!MetaDataHelperManager.getInstance().hasTable(name)) {
                throw new ServiceException("table " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }
            long start = System.currentTimeMillis();
            stateLog.info("compact table " + name + " start ");
            HbaseServiceManager.getInstance().getAdminService().compact(name);
            stateLog.info("compact table " + name + " success use:" + (System.currentTimeMillis() - start) + "ms");

            return success;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}
