package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by mac on 2017/4/19.
 */
public class AbstractService implements Closeable {

    Logger log = Logger.getLogger(getClass());

    protected HbaseServiceManager serviceManager;

    public AbstractService(HbaseServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    protected HTable getTable(String tableName) throws IOException {
        return (HTable) this.serviceManager.getHBaseConnection().getTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() throws IOException {

    }

    public <T> T execute(String tableName, TableCallback<T> action) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (table == null) {
                throw new RuntimeException("Exception table not exist, tableName=" + tableName + "\n");
            }

            T result = action.doWithTable(table);

            return result;
        } catch (RuntimeException e) {
            throw e;
        } catch (RetriesExhaustedException e) {
            if (e.getMessage().contains("org.apache.hadoop.hbase.exceptions.ConnectionClosingException")) {
                serviceManager.reconnect();
            }

            log.error("connect region server occur error,nested exception:" + e.getMessage() + ".");
            throw new RuntimeException("connect region server occur error, caused by " + e.getMessage());
        } catch (NoServerForRegionException e) {
            log.error("region occur error, cased by no server for region.");
            throw new RuntimeException("searching region occur error, caused by ", e);
        } catch (IOException e) {
            throw new RuntimeException("search table " + tableName + " occur error, caused by ", e);
        } catch (Exception e) {
            throw new RuntimeException("Exception occured in execute method, tableName=" + tableName + "\n", e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
    }

    public <T> T execute(AdminCallBack<T> action) {

        try {
            Admin admin = this.serviceManager.getHBaseConnection().getAdmin();

            T result = action.doWithAdmin(admin);

            return result;
        } catch (RuntimeException e) {
            throw e;
        } catch (RetriesExhaustedException e) {
            if (e.getMessage().contains("org.apache.hadoop.hbase.exceptions.ConnectionClosingException")) {
                HbaseServiceManager.getInstance().reconnect();
            }
            log.error("connect region server occur error,nested exception:" + e.getMessage() + ".");

            throw new RuntimeException("connect region server occur error, caused by " + e.getMessage());
        } catch (NoServerForRegionException e) {
            log.error("region occur error, cased by no server for region.");
            throw new RuntimeException("searching region occur error, caused by ", e);
        } catch (Exception e) {
            throw new RuntimeException("Exception occured in execute method\n", e);
        } finally {

        }
    }
}
