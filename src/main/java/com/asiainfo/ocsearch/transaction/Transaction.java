package com.asiainfo.ocsearch.transaction;

/**
 * Created by mac on 2017/3/26.
 */
public interface Transaction {

     void rollBack();
     void execute();
     void add(AtomicOperation atomicOperation);

}
