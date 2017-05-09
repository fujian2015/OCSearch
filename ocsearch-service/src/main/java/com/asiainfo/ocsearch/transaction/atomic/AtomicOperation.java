package com.asiainfo.ocsearch.transaction.atomic;

import java.io.Serializable;

/**
 * Created by mac on 2017/3/26.
 */
public interface AtomicOperation extends Serializable{
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
