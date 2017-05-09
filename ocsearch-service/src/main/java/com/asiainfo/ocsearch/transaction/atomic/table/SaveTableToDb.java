package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.meta.Table;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.SqlUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by mac on 2017/5/2.
 */
public class SaveTableToDb implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    Table table;

    public SaveTableToDb(Table table) {
        this.table = table;
    }

    @Override
    public boolean execute() {
        try {

            log.info("save table to mysql" +table.getName()+ " start!");
            MyBaseService myBaseService = MyBaseService.getInstance();

            Map<String, Object> tableMap = generateTable();

            String insertSchema = SqlUtil.prepareSql("table_def", tableMap.keySet());

            myBaseService.insertOrUpdate(insertSchema, tableMap.values().toArray());

            log.info("save table to mysql" +table.getName()+ " success!");
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("insert " + table.getName() + " to db failure", e);
        }

        return true;
    }

    @Override
    public boolean recovery() {
        try {
            String deleteTable = "delete  from  table_def where `name` = '" + table.getName() + "'";

            log.info(deleteTable + " start!");
            MyBaseService myBaseService = MyBaseService.getInstance();
            myBaseService.delete(deleteTable);
            log.info(deleteTable + " success!");
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("delete " + table.getName() + " from db failure", e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String queryTable = "select * from  table_def where `name` = '" + table.getName() + "'";

            Map<String, Object> result = myBaseService.queryOne(queryTable);

            if (result == null || result.isEmpty())
                return true;

            log.warn("table " + table.getName() + " has existed!");

        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("check " + table.getName() + " from db failure", e);
        }
        return false;
    }

    /**
     * String name;
     * String schema;
     * int hbaseRegions = -1;
     * int solrShards = -1;
     * int solrReplicas = -1;
     * TreeSet<String> regionSplits ;
     *
     * @return
     */
    public Map<String, Object> generateTable() {

        Map<String, Object> tableMap = new TreeMap();

        tableMap.put("name", table.getName());
        tableMap.put("schema_name", table.getSchema());
        tableMap.put("hbase_regions", table.getHbaseRegions());
        tableMap.put("solr_shards", table.getSolrShards());
        tableMap.put("solr_replicas", table.getSolrReplicas());

        ArrayNode splits = JsonNodeFactory.instance.arrayNode();

        TreeSet<String> regionSplits = table.getRegionSplits();

        if (regionSplits != null)
            regionSplits.forEach(split -> splits.add(split));

        tableMap.put("region_splits", splits.toString());

        return tableMap;
    }
}
