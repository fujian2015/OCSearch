package com.asiainfo.ocsearch.transaction;

import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;

/**
 * Created by mac on 2017/3/26.
 */
public interface Transaction {

     void rollBack();
     void execute();
     void add(AtomicOperation atomicOperation);
     boolean canExecute();

}
