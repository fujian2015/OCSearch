package com.asiainfo.ocsearch.transaction.atomic;


import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.transaction.Transaction;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by mac on 2017/3/26.
 */
public class TransactionImpl implements Transaction, Serializable {

    private Queue<AtomicOperation> ops = new ConcurrentLinkedQueue();

    private List<AtomicOperation> completedOps = new LinkedList<AtomicOperation>();

    public void rollBack() {

        for (int i = completedOps.size() - 1; i >= 0; i--) {

            AtomicOperation operation = completedOps.remove(i);

            try {
                operation.recovery();
            } catch (Exception e) {
                completedOps.add(operation);
                throw new RuntimeException("rollBack failure!", e);
            }
        }
    }

    public void execute() {

        AtomicOperation operation;

        while (null != (operation = ops.poll())) {

            completedOps.add(operation);
            operation.execute();
        }
    }

    public void add(AtomicOperation atomicOperation) {
        ops.offer(atomicOperation);
    }

    @Override
    public boolean canExecute() {

        boolean canExecute =true;
        for (AtomicOperation atomicOperation : ops) {
            canExecute = atomicOperation.canExecute();
            if (!canExecute) break;
        }
        return canExecute;
    }

}
