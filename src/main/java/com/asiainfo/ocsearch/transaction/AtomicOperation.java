package com.asiainfo.ocsearch.transaction;

/**
 * Created by mac on 2017/3/26.
 */
public interface AtomicOperation {
    /**
     * excute the operation
     */
     boolean execute();

    /**
     * 恢复到execute函数执行前的状态
     */
     boolean recovery();
    /**
     * 检查操作是否可行;
     */
    boolean canExecute();


}
