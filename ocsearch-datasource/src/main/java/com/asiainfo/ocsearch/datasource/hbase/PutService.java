package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by mac on 2017/4/19.
 */
public class PutService extends  AbstractService{

    Logger log = Logger.getLogger(getClass());

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
