package com.asiainfo.ocsearch.scheduler;

import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by mac on 2017/5/7.
 */
public class ProcessTransaction implements Runnable {

    Logger log = Logger.getLogger(getClass());

    @Override
    public void run() {
        doProcess();
    }

    public void doProcess() {

        List<String> transactions = TransactionUtil.getTranactions(false);

        for (String name : transactions) {
            Transaction transaction;
            try {
                transaction = TransactionUtil.deSerialize(name, false);
            } catch (ServiceException e) {
                log.error(e);
                continue;
            }
            try {
                transaction.execute();
            } catch (Exception e) {
                log.error(e);
                TransactionUtil.deleteTransaction(name, false);
                try {
                    TransactionUtil.serialize(name, transaction, false);
                } catch (ServiceException se) {
                    log.error(se);
                }
            }
        }
    }
}
