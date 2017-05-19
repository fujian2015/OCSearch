package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.schema.CreateSolrConfig;
import com.asiainfo.ocsearch.transaction.atomic.schema.SaveSchemaToZk;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/3/21.
 */
public class AddSchemaService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        try {

            stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());
            Schema tableSchema = new Schema(request);

            if (MetaDataHelperManager.getInstance().hasSchema(tableSchema.getName())) {
                throw new ServiceException(String.format("schema :%s  exists.", tableSchema.name), ErrorCode.SCHEMA_EXIST);
            }

            Transaction transaction = new TransactionImpl();

            if (tableSchema.getIndexType() != IndexType.HBASE_ONLY) {
                transaction.add(new CreateSolrConfig(tableSchema));
//                transaction.add(new CreateIndexerConfig(tableSchema));
            }

//            transaction.add(new SaveSchemaToDb(tableSchema));
//            transaction.add(new AddSchemaToMemory(tableSchema));
            transaction.add(new SaveSchemaToZk(tableSchema)); //instead db with zookeeper

            String transactionName = uuid + "_schema_add_" + tableSchema.name;

            if (!transaction.canExecute()) {
                throw new ServiceException(String.format("add schema :%s  failure.", tableSchema.name), ErrorCode.RUNTIME_ERROR);
            }
            try {
                transaction.execute();
            } catch (Exception e) {
                try {
                    transaction.rollBack();
                } catch (Exception rollBackException) {
                    log.error("roll back adding schema " + tableSchema.name + " failure", rollBackException);
                    TransactionUtil.serialize(transactionName, transaction, true);
                }
                throw e;
            }
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }
        return success;
    }

}
