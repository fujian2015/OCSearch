package com.asiainfo.ocsearch.transaction.atomic;

import com.asiainfo.ocsearch.datasource.mysql.MyBaseService;
import com.asiainfo.ocsearch.meta.Table;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.SqlUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by mac on 2017/5/2.
 */
public class SaveTableToDb implements AtomicOperation, Serializable {

    private static Logger logger = Logger.getLogger(SaveTableToDb.class);

    Table table;

    public SaveTableToDb(Table table) {
        this.table = table;
    }

    @Override
    public boolean execute() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            Map<String, Object> tableMap = generateTable();

            String insertSchema = SqlUtil.prepareSql("table_def", tableMap.keySet());

            myBaseService.insertOrUpdate(insertSchema, tableMap.values().toArray());

        } catch (SQLException e) {
            logger.error(e);
            throw new RuntimeException("insert " + table.getName() + " to db failure", e);
        }

        return true;
    }

    @Override
    public boolean recovery() {
        try {
            MyBaseService myBaseService = MyBaseService.getInstance();

            String deleteTable = "delete  from  table_def where `name` = '" + table.getName() + "'";

            myBaseService.delete(deleteTable);

        } catch (SQLException e) {
            logger.error(e);
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

            logger.warn("table " + table.getName() + " has existed!");

        } catch (SQLException e) {
            logger.error(e);
            throw new RuntimeException("check " + table.getName() + " from db failure", e);
        }
        return false;
    }

    /**
     * String name;
     * String schema;
     * IndexType indexType ;  //-1: hbase ,0 solr+hbase hbase-indexer
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
        tableMap.put("index_type", table.getIndexType().getValue());
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
