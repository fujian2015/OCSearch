package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.hbase.AdminService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import org.apache.log4j.Logger;

/**
 * Created by mac on 2017/5/5.
 */
public class DeleteHbaseTable implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String table;
    public DeleteHbaseTable(String table) {
        this.table = table;
    }

    @Override
    public boolean execute() {

        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();

        if (adminService.existsTable(table)) {
            log.info("delete hbase table " + table + " start!");
            adminService.deleteTable(table);
            log.info("delete hbase table " + table + " success!");
        }
        return true;
    }

    @Override
    public boolean recovery() {
        return true;
    }

    @Override
    public boolean canExecute() {
        return true;
    }
}
