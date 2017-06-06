package com.asiainfo.ocsearch.datainput.util;


import com.asiainfo.ocsearch.meta.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aaron on 17/6/1.
 */
public class SchemaProvider {

    private String tableName;
    private Schema schema;
    private String rowkeyExpression;
    private Map<String,InnerField> innerFields;
    private Map<String,Field> fields;
    private Map<String,Integer> fieldSequenceMap;


    public SchemaProvider(String tableName, Map<String,Integer> fieldSequenceMap)
    {
        this.tableName = tableName;
        this.fieldSequenceMap = fieldSequenceMap;
        this.schema = SchemaManager.getSchemaByTable(tableName);
        this.rowkeyExpression = schema.getRowkeyExpression();
        this.innerFields = schema.getInnerFields();
        this.fields = schema.getFields();
    }

    public SchemaProvider(Schema schema) {
        this.schema = schema;
        this.rowkeyExpression = schema.getRowkeyExpression();
        this.innerFields = schema.getInnerFields();
        this.fields = schema.getFields();
    }

    public void setFieldSequenceMap(Map<String, Integer> fieldSequenceMap) {
        this.fieldSequenceMap = fieldSequenceMap;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getRowkeyExpression() {
        return rowkeyExpression;
    }

    public Map<String,ColumnField> getColumnFamilyMap() {

        Map<String,ColumnField> columnFamilyMap = new HashMap<>();

        String columnFamily;

        String column;

        for(Map.Entry<String,InnerField> entry : this.innerFields.entrySet())
        {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();

            columnFamilyMap.put(columnFamily+":"+column,getColumnField(columnFamily,column));
        }
        for(Map.Entry<String,Field> entry : this.fields.entrySet())
        {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();

            if(entry.getValue().getHbaseFamily()!=null&&entry.getValue().getHbaseColumn()!=null)
                columnFamilyMap.put(columnFamily+":"+column,getColumnField(columnFamily,column));
        }
        return columnFamilyMap;
    }

//    public Map<String,String> getColumnSequenceMap() {
//        Map<String,String> columnSquenceMap = new HashMap<>();
//        for(Map.Entry<String,Field> entry : this.fields.entrySet())
//        {
//            if(entry.getValue().getHbaseFamily()!=null&&entry.getValue().getHbaseColumn()!=null)
//                columnSquenceMap.put(entry.getValue().getHbaseColumn(),fieldSequenceMap.get(entry.getValue().getName()).toString());
//        }
//        for(Map.Entry<String,InnerField> entry : this.innerFields.entrySet())
//        {
//            String column = entry.getValue().getHbaseColumn();
//            String innerfieldName = entry.getValue().getName();
//            String seperator = entry.getValue().getSeparator();
//
//
//        }
//    }

    private ColumnField getColumnField(String columnFamily,String column) {
        ColumnField columnField;
        String name;
        String seperator;
        ArrayList<Integer> sequence = new ArrayList<>();
        FieldType type;
        for(Map.Entry<String,InnerField> entry : this.innerFields.entrySet())
        {
            name = entry.getKey();
            seperator = entry.getValue().getSeparator();
            type = FieldType.STRING;
            if(columnFamily.equals(entry.getValue().getHbaseFamily())&&column.equals(entry.getValue().getHbaseColumn()))
            {
                int innerIndex = 0;
                outer:
                while(true){
                    for(Map.Entry<String,Field> entry1 : this.fields.entrySet())
                    {
                        if(name.equals(entry1.getValue().getInnerField())&&innerIndex==entry1.getValue().getInnerIndex())
                        {
                            String fieldName = entry1.getKey();
                            sequence.add(fieldSequenceMap.get(fieldName));
                            innerIndex++;
                            continue outer;
                        }
                    }
                    break;
                }
                columnField = new ColumnField(column,sequence,type);
                columnField.setSeperator(seperator);
                return columnField;
            }
        }
        for(Map.Entry<String,Field> entry : this.fields.entrySet())
        {
            if(columnFamily.equals(entry.getValue().getHbaseFamily())&&column.equals(entry.getValue().getHbaseColumn()))
            {
                type = entry.getValue().getStoreType();
                sequence.add(fieldSequenceMap.get(entry.getKey()));
                columnField = new ColumnField(column,sequence,type);
                return columnField;
            }
        }
        return null;
    }

}
