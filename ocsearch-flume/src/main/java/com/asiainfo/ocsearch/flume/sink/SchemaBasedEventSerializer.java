package com.asiainfo.ocsearch.flume.sink;

import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import com.asiainfo.ocsearch.flume.util.HttpRestFulClient;
import com.asiainfo.ocsearch.flume.util.PayloadConverter;
import com.asiainfo.ocsearch.flume.util.RowPutGenerator;
import com.asiainfo.ocsearch.meta.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Aaron on 17/6/30.
 */
public class SchemaBasedEventSerializer implements OCHbaseEventSerializer {

    private String baseUrl;
    private String targetUrl;
    private String schemaName;
    private Schema schema = null;
    private String inputFormat;
    private String csvSeparator;
    private String attachmentSeparator;
    private String fieldSequenceStr;
    private String fileRootFolder;
    private byte[] payload;
    private Map<String, String> headers;
    private String fileName;
    private String rowKeyExpression;
    private String tableExpression;
    private Engine expressionEngine = Engine.getInstance();
    private Executor rowKeyExecutor;
    private Executor tableNameExecutor = null;
    private Map<String,Object> dataMap;
    private RowPutGenerator rowPutGenerator;
    private PayloadConverter payloadConverter;
    private boolean isSpecifiedTable;
    private String specifiedTableName = null;

    @Override
    public void initialize(Event var1) {

        this.headers = var1.getHeaders();
        this.payload = var1.getBody();

        try {
            dataMap = payloadConverter.prepareData(this.payload);
        }catch (Exception e) {
            e.printStackTrace();
        }
        this.fileName = headers.get("file");
        this.fileName = fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());

    }

    @Override
    public Map<String, List<Row>> getActions() {
//        return null;
        ArrayList actions = Lists.newArrayList();
        Map<String,List<Row>> actionMap = new HashMap<>();
        String tableName;
        byte[] rowKeyBytes = Bytes.toBytes(rowKeyExecutor.evaluate(dataMap).toString());
        if(specifiedTableName != null) {
            tableName = specifiedTableName;
        }else {
            tableName = tableNameExecutor.evaluate(dataMap).toString();
        }

        try {
            Put put = rowPutGenerator.generatePut(dataMap,rowKeyBytes,fileName);
            if(put == null) {
                return null;
            }
            actions.add(put);
            actionMap.put(tableName,actions);
        }catch (Exception e) {

        }
        return actionMap;

    }

    @Override
    public Map<String, List<Increment>> getIncrements() {
        Map<String,List<Increment>> map = new HashMap<>();
        return map;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        this.baseUrl = context.getString("OCSearchServerURL","http://127.0.0.1:8080");
        this.schemaName = context.getString("schema","");
        this.inputFormat = context.getString("InputFormat","json");
        this.csvSeparator = context.getString("CsvSeparator",",");
        this.fieldSequenceStr = context.getString("FieldSequence","");
        this.fileRootFolder = context.getString("FileRootFolder","/");
        this.attachmentSeparator = context.getString("AttachmentSeparator",",");
        this.specifiedTableName = context.getString("tablename");
        this.targetUrl = this.baseUrl + "/schema/get?type=schema&name=" + this.schemaName;
        this.schema = getSchema();
        Preconditions.checkNotNull(this.schema,"Cannot get schema from server, plz check config");
        this.rowKeyExpression = schema.getRowkeyExpression();
        this.tableExpression = schema.getTableExpression();
        this.rowKeyExecutor = expressionEngine.createExecutor(rowKeyExpression);
        if((!tableExpression.equals(""))&&specifiedTableName==null) {
            this.tableNameExecutor = expressionEngine.createExecutor(tableExpression);
        }
        this.rowPutGenerator = new RowPutGenerator(schema);
        rowPutGenerator.setFileRootFolder(fileRootFolder);
        rowPutGenerator.setAttachmentSeparator(attachmentSeparator);
        Map<String,Integer> fieldSequence = null;
        try {
//            Map<String,Integer> fieldSequence = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(this.fieldSequenceStr);
            fieldSequence = mapper.convertValue(jsonNode,Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(csvSeparator.equals("|")) {
            csvSeparator = "\\|";
        }
        this.payloadConverter = new PayloadConverter(this.inputFormat,this.csvSeparator,fieldSequence);


    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }

    private Schema getSchema() {
        JsonNode jsonNode = HttpRestFulClient.getRequest(targetUrl);
        Schema schema = null;
        jsonNode = jsonNode.get("schema");
        try {
            schema = new Schema(jsonNode);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        return schema;
    }
}
