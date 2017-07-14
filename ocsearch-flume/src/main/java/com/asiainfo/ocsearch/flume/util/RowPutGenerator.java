package com.asiainfo.ocsearch.flume.util;

import com.asiainfo.ocsearch.meta.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Aaron on 17/7/3.
 */
public class RowPutGenerator {

    private Schema schema;
    private Map<String,ColumnField> columnFieldMap;
    private String fileRootFolder;
    private String attachmentSeparator;


    public RowPutGenerator(Schema schema) {

        this.schema = schema;
        initialMap();

    }
    public void setFileRootFolder(String fileRootFolder) {
        this.fileRootFolder = fileRootFolder;
    }

    public void setAttachmentSeparator(String attachmentSeparator) {
        this.attachmentSeparator = attachmentSeparator;
    }

    private void initialMap() {
        columnFieldMap = new HashMap<>();
        String columnFamily;
        String column;
        Map<String, InnerField> innerFieldMap = schema.getInnerFields();
        Map<String,Field> fieldMap = schema.getFields();
        for(Map.Entry<String,InnerField> entry : innerFieldMap.entrySet()) {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();
            String innerFieldName = entry.getKey();
            String innerFieldSeparater = entry.getValue().getSeparator();
            ArrayList<String> fieldList = new ArrayList<>();
            int innerIndex = 0;
            outer:
            while(true){
                for(Map.Entry<String,Field> entry1 : fieldMap.entrySet())
                {
                    if(innerFieldName.equals(entry1.getValue().getInnerField())
                            &&innerIndex==entry1.getValue().getInnerIndex())
                    {
                        String fieldName = entry1.getKey();
                        fieldList.add(fieldName);
                        innerIndex++;
                        continue outer;
                    }
                }
                break;
            }
            columnFieldMap.put(columnFamily+":"+column,
                    new ColumnField(innerFieldSeparater,FieldType.STRING,fieldList));
        }
        for(Map.Entry<String,Field> entry : fieldMap.entrySet()) {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();
            FieldType type = entry.getValue().getStoreType();
            if(columnFamily!=null&&column!=null) {
                ArrayList<String> fieldList = new ArrayList<>();
                fieldList.add(entry.getKey());
                columnFieldMap.put(columnFamily+":"+column,new ColumnField(null,type,fieldList));
            }
        }
    }

    public Put generatePut(Map<String,Object> dataMap,byte[] rowkey,String fileName) {
        Put put = new Put(rowkey);
        String columnFamily;
        String column;
        String content;
        Map<String,String> exsitFilesMap = new HashMap<>();
        for(Map.Entry<String,ColumnField> entry : columnFieldMap.entrySet()) {
            String key = entry.getKey();
            ColumnField columnfield = entry.getValue();
            content = "";

            columnFamily = key.split(":")[0];
            column = key.split(":")[1];
            String separater = columnfield.getSeparater();
            FieldType type = columnfield.getType();
            ArrayList<String> fieldList = columnfield.getFieldList();
            if(separater == null) {//field
                String fieldName = fieldList.get(0);
                String value = String.valueOf(dataMap.get(fieldList.get(0)));
                switch (type) {
                    case FILE:
                        if(fileName!=null) {
                            String filePath = this.fileRootFolder+"/"+fileName+"/"+value;
                            try {
                                byte[] filebytes = GetFileUtil.getBytesfromLocalFile(filePath);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),filebytes);
                                if(filebytes!=null) {
                                    String existFiles;
                                    if(exsitFilesMap.containsKey(columnFamily)) {
                                        existFiles = exsitFilesMap.get(columnFamily) + fieldName + ",";
                                    }else {
                                        existFiles = fieldName + ",";
                                    }
                                    exsitFilesMap.put(columnFamily,existFiles);
                                }
                            }catch (Exception e) {

                            }
                        }else {
                            return null;
                        }
                        break;
                    case ATTACHMENT:
                        if(fileName != null) {
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                            String[] filePaths = value.split(this.attachmentSeparator);
                            int length = filePaths.length;
                            for(int i = 0;i<length;i++) {
                                String filePath = this.fileRootFolder+"/"+fileName+"/"+filePaths[i];
                                try {
                                    byte[] filebytes = GetFileUtil.getBytesfromLocalFile(filePath);
                                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(filePaths[i]),filebytes);
                                }catch (Exception e) {

                                }
                            }

                        }else {
                            return null;
                        }
                        break;
                    case INT:
                        if (FieldTypeChecker.isInteger(value)) {
                            try {
                                int intContent = Integer.parseInt(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(intContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    case DOUBLE:
                        if (FieldTypeChecker.isDouble(value)) {
                            try {
                                double doubleContent = Double.parseDouble(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(doubleContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    case FLOAT:
                        if (FieldTypeChecker.isDouble(value)) {
                            try {
                                float floatContent = Float.parseFloat(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(floatContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    default:
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            } else {
                Iterator<String> iterator = fieldList.iterator();
                while (iterator.hasNext()) {
                    content = content.concat((String) dataMap.get(iterator.next())).concat(separater);
                }
                content = content.substring(0,content.lastIndexOf(separater));
                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(content));
            }

        }
        for (Map.Entry<String,String> entry : exsitFilesMap.entrySet()) {
            String columFamily = entry.getKey();
            String existFiles = entry.getValue();
            existFiles = existFiles.substring(0,existFiles.lastIndexOf(","));
            put.addColumn(Bytes.toBytes(columFamily),Bytes.toBytes("EXISTFILES"),Bytes.toBytes(existFiles));

        }
        return put;
    }


    public Put generatePut(Schema schema,Map<String,String> dataMap,byte[] rowkey) {

        Put put = new Put(rowkey);
        String columnFamily;
        String column;
        String content;
        Map<String, InnerField> innerFieldMap = schema.getInnerFields();
        Map<String,Field> fieldMap = schema.getFields();

        for(Map.Entry<String,InnerField> entry : innerFieldMap.entrySet()) {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();
            content = "";
            String innerFieldName = entry.getKey();
            String innerFieldSeparater = entry.getValue().getSeparator();
            int innerIndex = 0;
            outer:
            while(true){
                for(Map.Entry<String,Field> entry1 : fieldMap.entrySet())
                {
                    if(innerFieldName.equals(entry1.getValue().getInnerField())
                            &&innerIndex==entry1.getValue().getInnerIndex())
                    {
                        String fieldName = entry1.getKey();
                        content = content.concat(dataMap.get(fieldName)).concat(innerFieldSeparater);
                        innerIndex++;
                        continue outer;
                    }
                }
                break;
            }
            content = content.substring(0,content.lastIndexOf(innerFieldSeparater));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(content));
        }

        for(Map.Entry<String,Field> entry : fieldMap.entrySet()) {
            columnFamily = entry.getValue().getHbaseFamily();
            column = entry.getValue().getHbaseColumn();
            FieldType type = entry.getValue().getStoreType();
            if(columnFamily!=null&&column!=null) {
                String value = dataMap.get(entry.getKey());
                switch (type) {
                    case INT:
                        if (FieldTypeChecker.isInteger(value)) {
                            try {
                                int intContent = Integer.parseInt(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(intContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    case DOUBLE:
                        if (FieldTypeChecker.isDouble(value)) {
                            try {
                                double doubleContent = Double.parseDouble(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(doubleContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    case FLOAT:
                        if (FieldTypeChecker.isDouble(value)) {
                            try {
                                float floatContent = Float.parseFloat(value);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(floatContent));
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        } else {
                            return null;
                        }
                        break;
                    default:
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
        }
        return put;
    }
}
