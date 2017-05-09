package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.hbase.AdminService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

import java.util.Set;

/**
 * Created by mac on 2017/3/31.
 */
public class CreateHbaseTable implements AtomicOperation {

    static  Logger log = Logger.getLogger("state");

    int regions;
    String table;
    Set<String> columnFamilies;
    Set<String> regionSplits;

    public CreateHbaseTable(String table, int regions, Set<String> regionSplits, Set<String> columnFamilies) {

        this.regions = regions;
        this.table = table;
        this.columnFamilies = columnFamilies;
        this.regionSplits = regionSplits;
    }

    @Override
    public boolean execute() {

        log.info("create hbase table " + table + " start!");

        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();

        if (regionSplits == null || regionSplits.isEmpty()) {
            adminService.createTable(table, columnFamilies, regions);
        } else {
            adminService.createTable(table, columnFamilies, regionSplits);
        }

        log.info("create hbase table " + table + " success!");

        return true;
    }

    @Override
    public boolean recovery() {

        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();

        if (adminService.existsTable(table)) {

            log.info("delete hbase table " + table + " start!");

            adminService.deleteTable(table);

            log.info("delete hbase table " + table + " success!");
        }
        return true;
    }

    @Override
    public boolean canExecute() {

        log.info("check hbase table" + table + " start!");

        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();

        boolean exists = adminService.existsTable(table);

        log.info("check hbase table " + table + " success!");

        if (exists)
            log.warn(" hbase table " + table + " exists ");

        return !exists;
    }
}
