package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableConfig;
import com.asiainfo.ocsearch.db.mysql.MyBaseService;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Created by mac on 2017/3/29.
 */
public class SaveConfigToDb implements AtomicOperation, Serializable {

    final static String[] tableFields = {"name", "hbase_table", "solr_collection",
           "store_type",  "store_period","partition"};

    final static String[] schemaFields = {"name", "indexed", "contented",
            "stored", "hbase_column", "hbase_family,table_name"};

    final static String[] baseFields = {"name", "isFast", "table_name"};
    final static String[] queryFields = {"name", "weight", "table_name"};


    TableConfig tableConfig;

    transient MyBaseService myBaseService;

    public SaveConfigToDb(TableConfig tableConfig) {

        this.tableConfig = tableConfig;
        this.myBaseService = MyBaseService.getInstance();
    }

    public boolean execute() {

        String insertTable = "insert into tables(" + StringUtils.join(tableFields, ",")
                + ")values("
                + tableConfig.name + "," + tableConfig.hbaseTbale + "," + tableConfig.solrCollection
                + "," + tableConfig.storeType + "," + tableConfig.storePeriod
                + ");";

        String insertSchema = "insert into schemas(" + StringUtils.join(schemaFields, ",") + ")values(?,?,?,?,?,?,?);";

        String insertBase= "insert into base(" + StringUtils.join(baseFields, ",") + ")values(?,?,?);";
        String insertQuery= "insert into query(" + StringUtils.join(queryFields, ",") + ")values(?,?,?);";

        try {
            myBaseService.insertOrUpdate(insertTable, tableConfig.getTableFields());
            myBaseService.batch(insertSchema, tableConfig.getSchemaFields());
            if(!tableConfig.baseFields.isEmpty())
                myBaseService.batch(insertBase, tableConfig.getBaseFields());
            if(!tableConfig.queryFields.isEmpty())
                myBaseService.batch(insertQuery, tableConfig.getQueryFields());

        } catch (SQLException e) {
            throw new RuntimeException("insert " + tableConfig.name + " to db failure", e);
        }

        return true;
    }

    public boolean recovery() {

        String deleteTable = "delete * from  tables where name = " + tableConfig.name;
//        String deleteSchema = "delete * from  schemas where table_name = " + tableConfig.name;
//        String deleteBase = "delete * from  base where table_name = " + tableConfig.name;
//        String deleteQuery = "delete * from  query where table_name = " + tableConfig.name;

        try {
//            myBaseService.delete(deleteQuery);
//            myBaseService.delete(deleteBase);
//            myBaseService.delete(deleteSchema);
            myBaseService.delete(deleteTable);

        } catch (SQLException e) {
            throw new RuntimeException("delete " + tableConfig.name + " from db failure", e);
        }
        return false;
    }
}
