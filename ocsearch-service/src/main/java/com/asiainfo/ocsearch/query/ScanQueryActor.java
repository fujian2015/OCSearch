package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.datasource.hbase.ScanService;
import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/18.
 */
public class ScanQueryActor extends QueryActor {

    static Logger logger = Logger.getLogger(ScanQueryActor.class);

    public ScanQueryActor(HbaseQuery hbaseQuery, CountDownLatch runningThreadNum) {

        super(hbaseQuery, runningThreadNum);
    }

    @Override
    public void run() {
        try {


            List<Pair<byte[], byte[]>> columns = this.hbaseQuery.getColumns();

            UniqueKeyFormatter uniqueKeyFormatter = hbaseQuery.getUniqueKeyFormatter();
            Scan scan = new Scan(uniqueKeyFormatter.unformatRow(hbaseQuery.getStartKey()), uniqueKeyFormatter.unformatRow(hbaseQuery.getStopKey()));

            columns.forEach(column -> scan.addColumn(column.getFirst(), column.getSecond()));


            scan.setBatch(Integer.parseInt(OCSearchEnv.getEnvValue("query.batch.size", "1000")));//一次传送会多少列
            scan.setCaching(Integer.parseInt(OCSearchEnv.getEnvValue("query.cache.size", "100")));//一次传送多少行

            if (StringUtils.isBlank(hbaseQuery.condition)) {
                executeWithoutCondition(scan);
            } else {
                executeWithCondition(scan);
            }

        } catch (Exception e) {
            logger.error("occur error in scan query,exceptions:", e);
            this.queryResult.setLastError(e);
        } finally {
            runningThreadNum.countDown();
        }
    }

    private void executeWithCondition(Scan scan) {

        ScanService scanService = HbaseServiceManager.getInstance().getScanService();
        Executor ee = Engine.getInstance().createExecutor(hbaseQuery.condition);

        List<ObjectNode> resultSet = scanService.execute(this.hbaseQuery.table, htable -> {
            List<ObjectNode> results = new ArrayList<>();
            ResultScanner scanner = null;
            int count = 0;
            try {
                scanner = htable.getScanner(scan);
                if (hbaseQuery.needTotal) {
                    for (Result result : scanner) {
                        Map<String, Object> dataMap = this.hbaseQuery.extractResult2Map(result);
                        try {
                            if (ee.evaluate(dataMap).equals(true)) {
                                count++;
                                if (count <= hbaseQuery.limit) {
                                    results.add(this.hbaseQuery.extractResult(result));
                                } else if (count == hbaseQuery.limit + 1) {
                                    this.queryResult.setnextRowkey(Bytes.toString(result.getRow()));
                                }
                            }
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    queryResult.setTotal(count);
                } else {
                    for (Result result : scanner) {
                        Map<String, Object> dataMap = this.hbaseQuery.extractResult2Map(result);
                        try {
                            if (ee.evaluate(dataMap).equals(true)) {
                                count++;
                                if (count <= hbaseQuery.getSkip()) {
                                    continue;
                                } else if (count > hbaseQuery.limit + hbaseQuery.skip) {
                                    this.queryResult.setnextRowkey(Bytes.toString(result.getRow()));
                                    break;
                                } else {
                                    results.add(this.hbaseQuery.extractResult(result));
                                }
                            }
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    queryResult.setTotal(results.size());
                }

            } catch (IOException e) {
                throw e;
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
            return results;
        });
        resultSet.forEach(result -> this.queryResult.addData(result));
    }

    private void executeWithoutCondition(Scan scan) {
        ScanService scanService = HbaseServiceManager.getInstance().getScanService();
        List<ObjectNode> resultSet = scanService.execute(this.hbaseQuery.table, htable -> {
            List<ObjectNode> results = new ArrayList<>();
            ResultScanner scanner = null;
            int count = 0;
            try {
                scanner = htable.getScanner(scan);
                if (hbaseQuery.needTotal) {
                    for (Result result : scanner) {
                        count++;
                        if (count <= hbaseQuery.limit) {
                            results.add(this.hbaseQuery.extractResult(result));
                        } else if (count == hbaseQuery.limit + 1) {
                            this.queryResult.setnextRowkey(Bytes.toString(result.getRow()));
                        }

                    }
                    queryResult.setTotal(count);
                } else {
                    for (Result result : scanner) {

                        count++;
                        if (count <= hbaseQuery.getSkip()) {
                            continue;
                        } else if (count > hbaseQuery.limit + hbaseQuery.skip) {
                            this.queryResult.setnextRowkey(Bytes.toString(result.getRow()));
                            break;
                        } else {
                            results.add(this.hbaseQuery.extractResult(result));
                        }
                    }
                    queryResult.setTotal(results.size());
                }

            } catch (IOException e) {
                throw e;
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
            return results;
        });
        resultSet.forEach(result -> this.queryResult.addData(result));
    }

}
