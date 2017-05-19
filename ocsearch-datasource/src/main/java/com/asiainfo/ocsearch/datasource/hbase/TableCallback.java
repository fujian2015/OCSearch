package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.Table;

/**
 * Created by mac on 2017/5/17.
 */
public interface TableCallback<T> {
     T doWithTable(Table htable) throws Exception;
}
