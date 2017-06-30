package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by mac on 2017/4/19.
 */
public class PutService extends  AbstractService{

    HTable hTable=null;

    public PutService(HbaseServiceManager hbaseServiceManager, String table) throws IOException {
        super(hbaseServiceManager);
        try {
            hTable = getTable(table);
        } catch (IOException e) {
            log.error(e);
            throw e;
        }
    }
    public void close(){}

    public void flush(){}


}
