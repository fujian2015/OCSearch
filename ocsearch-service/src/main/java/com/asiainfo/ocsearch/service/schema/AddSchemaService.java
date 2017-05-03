package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.SchemaManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.TransactionUtil;
import com.asiainfo.ocsearch.transaction.atomic.SaveSchemaToDb;
import com.asiainfo.ocsearch.transaction.atomic.TransactionImpl;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/3/21.
 */
public class AddSchemaService extends OCSearchService {

    Logger log = Logger.getLogger(getClass());

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {
        String uuid = getRequestId();
        try {
            stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());
            Schema tableSchema = new Schema(request);

            if (SchemaManager.existsConfig(tableSchema.name)) {
                throw new ServiceException(String.format("schema :%s  exists.", tableSchema.name), ErrorCode.SCHEMA_EXIST);
            }

            Transaction transaction = new TransactionImpl();

            transaction.add(new SaveSchemaToDb(tableSchema));

            String transactionName = uuid + "_add_" + tableSchema.name;


            if (transaction.canExecute()) {
                try {
                    TransactionUtil.serialize(transactionName, transaction);
                    transaction.execute();
                    TransactionUtil.deleteTransaction(transactionName);
                    SchemaManager.addSchema(tableSchema.name, tableSchema);
                } catch (Exception e) {
                    try {
                        transaction.rollBack();
                    } catch (Exception rollBackException) {
                        log.error("roll back adding schema " + tableSchema.name + " failure", rollBackException);
                    }
                    throw e;
                }
            } else {
                throw new ServiceException(String.format("add schema :%s  failure.", tableSchema.name), ErrorCode.RUNTIME_ERROR);
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
