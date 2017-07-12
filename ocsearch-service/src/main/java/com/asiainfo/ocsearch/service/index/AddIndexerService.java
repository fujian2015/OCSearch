package com.asiainfo.ocsearch.service.index;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.table.CreateIndexerTable;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/6/30.
 */
public class AddIndexerService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

        try {

            String name = request.get("name").asText();

            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(name);

            if (schema == null) {
                throw new ServiceException("table : " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }

            Transaction transaction = new TransactionImpl();

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER || indexType == IndexType.HBASE_SOLR_BATCH) {

                transaction.add(new CreateIndexerTable(name, schema));
            } else {
                throw new ServiceException(String.format("table :%s  has a index_type,%s", name, indexType.toString()), ErrorCode.TABLE_NOT_EXIST);
            }

            if (!transaction.canExecute()) {
                throw new ServiceException(String.format("add indexer :%s  failure because of transaction can't be executed.", name), ErrorCode.RUNTIME_ERROR);
            }
            try {
                transaction.execute();
            } catch (Exception e) {
                log.error(e);
                try {
                    transaction.rollBack();
                } catch (Exception rollBackException) {
                    TransactionUtil.serialize(uuid + "_indexer_add_" + name, transaction, true);
                    log.error("roll back create table " + name + " failure", rollBackException);
                }
                throw e;
            }
            return success;
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}
