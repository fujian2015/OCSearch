package com.asiainfo.ocsearch.transaction;

/**
 * Created by mac on 2017/3/26.
 */
public interface AtomicOperation {
    /**
     * excute the operation
     */
    public boolean execute();

    /**
     * 恢复到execute函数执行前的状态
     */
    public boolean recovery();
}
