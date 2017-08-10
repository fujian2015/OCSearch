package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.table.DeleteIndexerTable;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/6/30.
 */
public class DeleteIndexerService extends OCSearchService {

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {
        String uuid = getRequestId();
        try {

            String name = request.get("name").asText();

            if (!MetaDataHelperManager.getInstance().hasTable(name)) {
                throw new ServiceException("table " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(name);

            Transaction transaction = new TransactionImpl();

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER || indexType == IndexType.HBASE_SOLR_PHOENIX) {
                transaction.add(new DeleteIndexerTable(name));
            } else {
                throw new ServiceException("indexer " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }

            if (!IndexerServiceManager.getIndexerService().exists(name)) {
                throw new ServiceException(String.format("indexer %s does not exist!", name), ErrorCode.INDEXER_NOT_EXIST);
            }

            try {
                transaction.execute();
            } catch (Exception e) {
                TransactionUtil.serialize(uuid + "_indexer_delete_" + name, transaction, false);
                log.error("delete indexer " + name + " failure", e);
                throw e;
            }
            return success;
        } catch (ServiceException se) {
            log.warn(se);
            throw se;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}