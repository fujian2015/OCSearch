package com.asiainfo.ocsearch.service.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.internal.GenerateSolrConfig;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/3/21.
 */
public class CreateTableService extends OCSearchService {

    Logger log = Logger.getLogger(getClass());

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();

        TableConfig tableConfig = new TableConfig(request);

        Transaction transaction = new TransactionImpl();

        transaction.add(new GenerateSolrConfig(tableConfig));

        String transactionName= uuid + "_create_" + tableConfig.name;

        try {
            TransactionUtil.serialize(transactionName,transaction);
            transaction.execute();
            TransactionUtil.deleteTranaction(transactionName);

        } catch (Exception e) {
            log.warn("create table " + tableConfig.name + " failure:", e);
            try {
                transaction.rollBack();
            } catch (Exception rollBackException) {
                log.error("roll back creating table " + tableConfig.name + " failure", rollBackException);
            }
            throw new ServiceException(e, ErrCode.RUNTIME_ERROR);
        }

        return success;
    }


}
