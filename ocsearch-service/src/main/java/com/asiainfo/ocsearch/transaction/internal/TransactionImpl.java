package com.asiainfo.ocsearch.transaction.internal;


import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;

import java.io.Serializable;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by mac on 2017/3/26.
 */
public class TransactionImpl implements Transaction, Serializable {

    private Deque<AtomicOperation> ops = new ConcurrentLinkedDeque();

    private Deque<AtomicOperation> completedOps = new ConcurrentLinkedDeque<>();

    public void rollBack() {

        AtomicOperation executingOp;

        while (null != (executingOp = completedOps.peekLast())) {

            executingOp.recovery();

            ops.offerFirst(completedOps.pop());
        }
    }

    public void execute() {

        AtomicOperation executingOp ;

        while (null != (executingOp = ops.peek())) {

            executingOp.execute();

            completedOps.push(ops.poll());
        }
    }

    public void add(AtomicOperation atomicOperation) {
        ops.offer(atomicOperation);
    }

    @Override
    public boolean canExecute() {

        boolean canExecute = true;
        for (AtomicOperation atomicOperation : ops) {
            canExecute = atomicOperation.canExecute();
            if (!canExecute) break;
        }
        return canExecute;
    }
}
