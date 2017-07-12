package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.atomic.table.UpdateIndexer;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/7/11.
 */
public class UpdateIndexerService extends OCSearchService {

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {
        try {
            String name = request.get("name").asText();

            if (!MetaDataHelperManager.getInstance().hasTable(name)) {
                throw new ServiceException("table " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(name);

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER || indexType == IndexType.HBASE_SOLR_BATCH) {
                new UpdateIndexer(name,schema).execute();
            } else {
                throw new ServiceException("indexer " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }

            return success;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}