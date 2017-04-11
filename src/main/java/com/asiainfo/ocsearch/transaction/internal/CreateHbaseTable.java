package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.db.hbase.HBaseService;
import com.asiainfo.ocsearch.db.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.log4j.Logger;

import java.util.Set;

/**
 * Created by mac on 2017/3/31.
 */
public class CreateHbaseTable implements AtomicOperation {

    final int regions;
    final String table;

    final Set<byte[]> columnFamilies;

    Logger log = Logger.getLogger(getClass());

    public CreateHbaseTable(int regions, String table, Set<byte[]> columnFamilies) {
        this.regions = regions;
        this.table = table;
        this.columnFamilies = columnFamilies;
    }


    @Override
    public boolean execute() {

        log.info("create hbase table "+table+" start!");

        HbaseServiceManager.gethBaseService().createTable(table, columnFamilies, regions);

        log.info("create hbase table "+table+" success!");

        return true;
    }

    @Override
    public boolean recovery() {

        HBaseService hBaseService = HbaseServiceManager.gethBaseService();

        if (hBaseService.existsTable(table)) {

            log.info("delete hbase table "+table+" start!");

            hBaseService.deleteTable(table);

            log.info("delete hbase table "+table+" success!");
        }
        return true;
    }

    @Override
    public boolean canExecute() {

        log.info("check hbase table"+table+" start!");

        HBaseService hBaseService = HbaseServiceManager.gethBaseService();

        boolean exists=hBaseService.existsTable(table);

        log.info("check hbase table "+table+" success!");

        return !exists;
    }
}
