package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.Admin;

/**
 * Created by mac on 2017/5/17.
 */
public interface AdminCallBack<T> {
    T doWithAdmin(Admin admin) throws Exception;
}
