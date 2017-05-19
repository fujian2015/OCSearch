package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.*;

/**
 * Created by mac on 2017/5/16.
 */
public class HbaseQuery {

    Schema schema;
    String table;

    Set<String> returnFields;

    Table<String, String, String> kvMap = HashBasedTable.create();

    List<Pair<byte[], byte[]>> columns = new ArrayList<>();

    public HbaseQuery(Schema schema, String table, Set<String> returnFields) {
        this.schema = schema;
        this.table = table;
        this.returnFields = returnFields;
        initial();
    }

    private void initial() {
        Map<String, Field> fields = schema.getFields();

        Set<String> innerNames = new HashSet<>();
        for (String name : returnFields) {
            // get attachment file  "name:test.txt" or get file "name:name"
            String file = null;
            if (name.contains(":")) {
                name = name.substring(0, name.indexOf(":"));
                file = name.substring(name.indexOf(":" + 1));
            }
            Field field = fields.get(name);

            if (file != null) {   //get attachment or file
                byte[] hbaseFamily = Bytes.toBytes(field.getHbaseFamily());
                byte[] hbaseColumn = Bytes.toBytes(file);
                columns.add(new Pair(hbaseFamily, hbaseColumn));
                kvMap.put(field.getHbaseFamily(), file, name);
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


    public JsonNode extractResult(Result result) {

        ObjectNode data = JsonNodeFactory.instance.objectNode();

        List<Cell> cells = result.listCells();

        Map<String, Field> fields = schema.getFields();

        for (Cell cell : cells) {

            String name = kvMap.get(Bytes.toString(cell.getFamilyArray()), Bytes.toString(cell.getQualifierArray()));
            byte[] valueArray = cell.getValueArray();

            if (returnFields.contains(name)) {
                if (fields.containsKey(name)) {
                    switch (fields.get(name).getStoreType()) {
                        case STRING:
                            data.put(name, Bytes.toString(valueArray));
                            break;
                        case INT:
                            data.put(name, Bytes.toInt(valueArray));
                            break;
                        case FLOAT:
                            data.put(name, Bytes.toFloat(valueArray));
                            break;
                        case BOOLEAN:
                            data.put(name, Bytes.toBoolean(valueArray));
                            break;
                        case FILE:
                            String names = Bytes.toString(valueArray);
                            for (String n : names.split(",")) {
                                if (returnFields.contains(n)) {
                                    data.put(n, generateFicelUrl(table,n,n));
                                }
                            }
                            break;
                        case ATTACHMENT:
                            ArrayNode attachNode = JsonNodeFactory.instance.arrayNode();
                            for (String file : Bytes.toString(valueArray).split(",")) {
                                attachNode.add(generateFicelUrl(table, name, file));
                            }
                            data.put(name, attachNode);
                            break;
                        default:
                            data.put(name, valueArray);
                    }
                } else {  //get attachment file
                    data.put(name, valueArray);
                }
            } else { //inner field
                String innerValue = Bytes.toString(valueArray);

                InnerField inf = schema.getInnerFields().get(name);
                String[] values = innerValue.split(inf.getSeparator());
                schema.getInnerMap()
                        .get(name)
                        .stream()
                        .filter(field -> returnFields.contains(field.getName()))
                        .forEach(field -> {
                                    String value = values[field.getInnerIndex()];
                                    if (StringUtils.isNotEmpty(value)) {
                                        switch (field.getStoreType()) {
                                            case INT:
                                                data.put(name, Integer.parseInt(value));
                                                break;
                                            case FLOAT:
                                                data.put(name, Float.parseFloat(value));
                                                break;
                                            case BOOLEAN:
                                                data.put(name, Boolean.parseBoolean(value));
                                                break;
                                            default:
                                                data.put(name, value);
                                        }
                                    }
                                }
                        );
            }
        }
        return data;
    }

    private String generateFicelUrl(String table, String field, String file) {
        return "&table=" + table + "&field=" + field + "&file=" + file;
    }


    public List<Pair<byte[], byte[]>> getColumns() {
        return columns;
    }
}
