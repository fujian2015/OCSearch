package com.asiainfo.ocsearch.scheduler;

import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by mac on 2017/5/7.
 */
public class RollBackTranscation implements Runnable {

    Logger log = Logger.getLogger(getClass());

    public void doRollback() {
        List<String> transactions = TransactionUtil.getTranactions(true);

        for (String name : transactions) {
            Transaction transaction;
            try {
                transaction = TransactionUtil.deSerialize(name, true);
            } catch (ServiceException e) {
                log.error(e);
                continue;
            }
            try {
                transaction.rollBack();
            } catch (Exception e) {
                TransactionUtil.deleteTransaction(name, true);
                try {
                    TransactionUtil.serialize(name, transaction, true);
                } catch (ServiceException se) {
                    log.error(se);
                }
            }
        }
    }

    @Override
    public void run() {
        doRollback();
    }
}
