package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import com.asiainfo.ocsearch.db.mysql.MyBaseService;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Set;

/**
 * Created by mac on 2017/3/29.
 */
public class SaveConfigToDb implements AtomicOperation, Serializable {

    TableConfig tableConfig;

    transient MyBaseService myBaseService;

    public SaveConfigToDb(TableConfig tableConfig) {

        this.tableConfig = tableConfig;
        this.myBaseService = new MyBaseService("config");
    }

    public boolean execute() {

        String insertTable="insert into tables" +
                "(name,hbase_table,solr_collection,hbase_indexer,store_type,auto_delete,partition)" +
                "values(" +
                tableConfig.name+","+tableConfig.hbaseTbale+","+tableConfig.solrCollection+","+tableConfig.storeType+","+tableConfig.storePeriod+
                ");";


        Set<String> keys=tableConfig.fields.keySet();
        String insertField="insert into schema" + StringUtils.join(tableConfig.fields.keySet(),",")+
                "values(" +
                tableConfig.name+","+tableConfig.hbaseTbale+","+tableConfig.solrCollection+","+tableConfig.storeType+","+tableConfig.storePeriod+
                ");";
        try {
            myBaseService.update(insertTable);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean recovery() {
        return false;
    }
}
