package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.*;

/**
 * Created by mac on 2017/5/16.
 */
public class HbaseQuery {

    static Logger log = Logger.getLogger(HbaseQuery.class);

    Schema schema;
    String table;
    String startKey;
    String stopKey;
    int limit;
    String condition;
    int skip = 0;
    boolean needTotal = false;
    UniqueKeyFormatter uniqueKeyFormatter;

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public boolean isNeedTotal() {
        return needTotal;
    }

    public void setNeedTotal(boolean needTotal) {
        this.needTotal = needTotal;
    }

    List<String> rowKeys;
    Set<String> returnFields;

    Table<String, String, String> kvMap = HashBasedTable.create();

    List<Pair<byte[], byte[]>> columns = new ArrayList<>();

    private Multimap<String, Field> innerMap = ArrayListMultimap.create();

    public HbaseQuery(Schema schema, String table, String startKey, String stopKey, int limit, String condition, Set<String> returnFields) {
        this.schema = schema;
        this.table = table;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.limit = limit;
        this.condition = condition;
        this.returnFields = returnFields;
        initial();
    }


    public HbaseQuery(Schema schema, String table, Set<String> returnFields, List<String> rowKeys) {
        this.schema = schema;
        this.table = table;
        this.returnFields = returnFields;
        this.rowKeys = rowKeys;
        initial();
    }

    private void initial() {

        uniqueKeyFormatter=RowkeyUtils.getIdFormatter(schema.getIdFormatter());

        Map<String, Field> fields = schema.getFields();

        Set<String> innerNames = new HashSet<>();
        for (String name : returnFields) {
            // get attachment file  "name:test.txt" or get file "name:name"
            String file = null;
            if (name.contains(":")) {

                file = name.substring(name.indexOf(":") + 1);
                name = name.substring(0, name.indexOf(":"));
            }
            Field field = fields.get(name);

            if (file != null) {   //get attachment or file
                byte[] hbaseFamily = Bytes.toBytes(field.getHbaseFamily());
                byte[] hbaseColumn = Bytes.toBytes(file);
                columns.add(new Pair(hbaseFamily, hbaseColumn));
                kvMap.put(field.getHbaseFamily(), file, name + ":" + file);
                continue;
            }
            if (field.getStoreType() == FieldType.FILE) {
                if (!kvMap.contains(field.getHbaseFamily(), Constants.FILE_NAMES_COLUMN)) {
                    byte[] hbaseFamily = Bytes.toBytes(field.getHbaseFamily());
                    columns.add(new Pair(hbaseFamily, Bytes.toBytes(Constants.FILE_NAMES_COLUMN)));
                    kvMap.put(field.getHbaseFamily(), Constants.FILE_NAMES_COLUMN, Constants.FILE_NAMES_COLUMN);
                }
                continue;
            }

            String inf = field.getInnerField();
            if (StringUtils.isEmpty(inf)) {
                byte[] hbaseFamily = Bytes.toBytes(field.getHbaseFamily());
                byte[] hbaseColumn = Bytes.toBytes(field.getHbaseColumn());
                columns.add(new Pair(hbaseFamily, hbaseColumn));
                kvMap.put(field.getHbaseFamily(), field.getHbaseColumn(), name);
            } else {
                innerNames.add(inf);
                innerMap.put(inf, field);
            }
        }

        Map<String, InnerField> innerFields = schema.getInnerFields();

        for (String name : innerNames) {

            InnerField innerField = innerFields.get(name);

            byte[] hbaseFamily = Bytes.toBytes(innerField.getHbaseFamily());
            byte[] hbaseColumn = Bytes.toBytes(innerField.getHbaseColumn());

            columns.add(new Pair<>(hbaseFamily, hbaseColumn));
            kvMap.put(innerField.getHbaseFamily(), innerField.getHbaseColumn(), name);
        }
    }


    public ObjectNode extractResult(Result result) {

        ObjectNode data = JsonNodeFactory.instance.objectNode();

        if(result.isEmpty())
            return data;

        String id= uniqueKeyFormatter.formatRow(result.getRow());

        data.put("id", id);

        Map<String, Field> fields = schema.getFields();

        columns.forEach(pair -> {
            byte[] valueArray = result.getValue(pair.getFirst(), pair.getSecond());

            String name = kvMap.get(Bytes.toString(pair.getFirst()), Bytes.toString(pair.getSecond()));
            if (valueArray == null)
                return;
            try {
                if (returnFields.contains(name)) {
                    if (fields.containsKey(name)) {
                        switch (fields.get(name).getStoreType()) {
                            case STRING:
                                data.put(name, Bytes.toString(valueArray));
                                break;
                            case INT:
                                data.put(name, Bytes.toInt(valueArray));
                                break;
                            case LONG:
                                data.put(name, Bytes.toLong(valueArray));
                                break;
                            case FLOAT:
                                data.put(name, Bytes.toFloat(valueArray));
                                break;
                            case DOUBLE:
                                data.put(name, Bytes.toDouble(valueArray));
                                break;
                            case BOOLEAN:
                                data.put(name, Bytes.toBoolean(valueArray));
                                break;
                            case ATTACHMENT:
                                ArrayNode attachNode = JsonNodeFactory.instance.arrayNode();
                                for (String file : Bytes.toString(valueArray).split(",")) {
                                    attachNode.add(generateFilelUrl(table, id, name, file));
                                }
                                data.put(name, attachNode);
                                break;
                            default:
                                data.put(name, valueArray);
                        }
                    } else {  //get attachment file
                        data.put(name, valueArray);
                    }
                } else if (StringUtils.equals(Constants.FILE_NAMES_COLUMN, name)) {
                    String names = Bytes.toString(valueArray);
                    for (String n : names.split(",")) {
                        if (returnFields.contains(n)) {
                            data.put(n, generateFilelUrl(table, id, n, fields.get(n).getHbaseColumn()));
                        }
                    }
                } else { //inner field
                    String innerValue = Bytes.toString(valueArray);

                    InnerField inf = schema.getInnerFields().get(name);
                    String[] values = innerValue.split(inf.getSeparator());
                    innerMap.get(name).forEach(field -> {
                                String value = values[field.getInnerIndex()];
                                if (StringUtils.isNotEmpty(value)) {
                                    switch (field.getStoreType()) {
                                        case INT:
                                            data.put(field.getName(), Integer.parseInt(value));
                                            break;
                                        case LONG:
                                            data.put(field.getName(), Long.parseLong(value));
                                            break;
                                        case FLOAT:
                                            data.put(field.getName(), Float.parseFloat(value));
                                            break;
                                        case DOUBLE:
                                            data.put(field.getName(), Double.parseDouble(value));
                                            break;
                                        case BOOLEAN:
                                            data.put(field.getName(), Boolean.parseBoolean(value));
                                            break;
                                        default:
                                            data.put(field.getName(), value);

                                    }
                                }
                            }
                    );
                }
            }catch (Exception e){
                log.warn("handle data exception:",e);
            }
        });

        return data;
    }

    private String generateFilelUrl(String table, String id, String field, String file) {
        return new FileID(table, field, id, file).toString();
    }


    public List<Pair<byte[], byte[]>> getColumns() {
        return columns;
    }

    public String getStartKey() {
        return startKey;
    }

    public String getStopKey() {
        return stopKey;
    }

    public Map extractResult2Map(Result result) {
        Map<String, Object> map = new HashMap<>();
        Map<String, Field> fields = schema.getFields();

        columns.forEach(pair -> {
            byte[] valueArray = result.getValue(pair.getFirst(), pair.getSecond());

            String name = kvMap.get(Bytes.toString(pair.getFirst()), Bytes.toString(pair.getSecond()));
            if (valueArray == null)
                return;

            if (returnFields.contains(name)) {
                if (fields.containsKey(name)) {
                    switch (fields.get(name).getStoreType()) {
                        case STRING:
                            map.put(name, Bytes.toString(valueArray));
                            break;
                        case INT:
                            map.put(name, Bytes.toInt(valueArray));
                            break;
                        case LONG:
                            map.put(name,Bytes.toLong(valueArray));
                            break;
                        case FLOAT:
                            map.put(name, Bytes.toFloat(valueArray));
                            break;
                        case DOUBLE:
                            map.put(name, Bytes.toDouble(valueArray));
                            break;
                        case BOOLEAN:
                            map.put(name, Bytes.toBoolean(valueArray));
                            break;
                        case FILE:
                        case ATTACHMENT:
                            break;
                    }
                }
            }
            else if (StringUtils.equals(Constants.FILE_NAMES_COLUMN, name)) {
               return;
            }
            else { //inner field
                String innerValue = Bytes.toString(valueArray);

                InnerField inf = schema.getInnerFields().get(name);
                String[] values = innerValue.split(inf.getSeparator());
                innerMap.get(name).forEach(field -> {
                            String value = values[field.getInnerIndex()];
                            if (StringUtils.isNotEmpty(value)) {
                                switch (field.getStoreType()) {
                                    case INT:
                                        map.put(field.getName(), Integer.parseInt(value));
                                        break;
                                    case LONG:
                                        map.put(field.getName(), Long.parseLong(value));
                                        break;
                                    case FLOAT:
                                        map.put(field.getName(), Float.parseFloat(value));
                                        break;
                                    case DOUBLE:
                                        map.put(field.getName(), Double.parseDouble(value));
                                        break;
                                    case BOOLEAN:
                                        map.put(field.getName(), Boolean.parseBoolean(value));
                                        break;
                                    default:
                                        map.put(field.getName(), value);

                                }
                            }
                        }
                );
            }
        });
        return map;
    }

    public List<String> getRowKeys() {
        return rowKeys;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public void setStopKey(String stopKey) {
        this.stopKey = stopKey;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public void setRowKeys(List<String> rowKeys) {
        this.rowKeys = rowKeys;
    }

    public Set<String> getReturnFields() {
        return returnFields;
    }

    public void setReturnFields(Set<String> returnFields) {
        this.returnFields = returnFields;
    }

    public Table<String, String, String> getKvMap() {
        return kvMap;
    }

    public void setKvMap(Table<String, String, String> kvMap) {
        this.kvMap = kvMap;
    }

    public void setColumns(List<Pair<byte[], byte[]>> columns) {
        this.columns = columns;
    }

    public UniqueKeyFormatter getUniqueKeyFormatter() {
        return uniqueKeyFormatter;
    }
}
