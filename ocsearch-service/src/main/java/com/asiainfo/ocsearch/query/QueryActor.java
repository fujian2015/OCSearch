package com.asiainfo.ocsearch.query;

import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/16.
 */
public abstract class QueryActor implements Runnable {

    QueryResult queryResult=new QueryResult();

    HbaseQuery hbaseQuery;

    CountDownLatch runningThreadNum;

    public QueryActor(HbaseQuery hbaseQuery,CountDownLatch runningThreadNum) {
        this.hbaseQuery = hbaseQuery;
        this.runningThreadNum = runningThreadNum;
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }

}
