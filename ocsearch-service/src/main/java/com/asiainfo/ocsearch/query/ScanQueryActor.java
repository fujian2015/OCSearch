package com.asiainfo.ocsearch.query;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/18.
 */
public class ScanQueryActor extends QueryActor {

    static Logger logger = Logger.getLogger(GetQueryActor.class);

    String startKey;
    String endKey;
    int limit;



    public ScanQueryActor(HbaseQuery hbaseQuery, String startKey, String endKey, CountDownLatch runningThreadNum,int limit) {

        super(hbaseQuery, runningThreadNum);
        this.startKey =startKey;
        this.endKey =endKey;
        this.limit = limit;
    }

}
