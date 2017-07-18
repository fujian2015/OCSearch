import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.flume.util.HttpRestFulClient;
import com.asiainfo.ocsearch.flume.util.PayloadConverter;
import com.asiainfo.ocsearch.flume.util.RowPutGenerator;
import com.asiainfo.ocsearch.meta.Schema;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by Aaron on 17/7/13.
 */
public class testSchemaEventSerializer {
    @Test
    public void testString2Map() {

        String fieldSequenceStr = "{\"test\":1,\"name\":2,\"sex\":3,\"phone\":4}";
        Map<String,Integer> fieldSequence = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(fieldSequenceStr);
            fieldSequence = mapper.convertValue(jsonNode,Map.class);
            System.out.println(fieldSequence.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testPayloadConverter() {
        PayloadConverter payloadConverter = new PayloadConverter("json",",",null);
        String str = "{\"test\":\"aaaa\",\"name\":\"wang\",\"sex\":\"male\",\"age\":20}";
        byte[] bytes = str.getBytes();
        Map<String,Object> result = payloadConverter.prepareData(bytes);
        System.out.println(result);

        String str1 = "4600092691075035|38136|61641|||||A0A211012||||||81402||||||||||13563522057";
        String separator = "\\|";
        Map<String,Integer> fieldSequence = new HashMap<>();
        fieldSequence.put("test",1);
        fieldSequence.put("name",2);
        fieldSequence.put("age",3);
        fieldSequence.put("imsi",8);
        PayloadConverter payloadConverter1 = new PayloadConverter("csv",separator,fieldSequence);
        Map<String,Object> result1 = payloadConverter1.prepareData(str1.getBytes());
        System.out.println(result1);


    }

    @Test
    public void testRowputGenerater() {
        String targetUrl = "http://10.1.236.66:28080/ocsearch-service/schema/get?type=schema&name=schemaYidong";
        JsonNode jsonNode = HttpRestFulClient.getRequest(targetUrl);
        jsonNode = jsonNode.get("schema");
        Schema schema = null;
        try {
            schema = new Schema(jsonNode);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        RowPutGenerator rowPutGenerator = new RowPutGenerator(schema);
        String str = "{\"PHONENUM\":\"13012345678\",\"SECURITY_AREA_IN\":\"1\",\"LONGITUDE\":\"2\",\"AGE_LEVEL\":\"20\"}";
        byte[] bytes = str.getBytes();
        PayloadConverter payloadConverter = new PayloadConverter("json",",",null);
        Map<String,Object> dataMap = payloadConverter.prepareData(bytes);

        ArrayList actions = Lists.newArrayList();
        Map<String,List<Row>> actionMap = new HashMap<>();
        byte[] rowKeyBytes = Bytes.toBytes("123");
        String tableName = "table";
        try {
            System.out.println(dataMap);
            Put put = rowPutGenerator.generatePut(dataMap,rowKeyBytes,"1.csv");
            System.out.println(put);
            actions.add(put);
            actionMap.put(tableName,actions);

            List<Cell> list = put.get("INFO".getBytes(),"AGE_LEVEL".getBytes());
            System.out.println(new String(list.get(0).getValue()));

        }catch (Exception e) {

        }
        System.out.println(actionMap);

    }

    @Test
    public void testFileName() {
        String fileName="/data/flume/spool/test/SITE.txt";
        fileName = fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
        System.out.println(fileName);
    }

    @Test
    public void testGetAction() {
        ArrayList actions = Lists.newArrayList();
        Map<String,List<Row>> actionMap = new HashMap<>();
        Put put = null;
        Put put1 = new Put(Bytes.toBytes("123"));
        put1.addColumn(Bytes.toBytes("1"),Bytes.toBytes("2"),Bytes.toBytes("3"));
//        actions.add(put);
        actions.add(put1);
        String tableName = "table";
        actionMap.put(tableName,actions);
        System.out.println(actionMap);
        actionMap = addMap(actionMap,null);
        System.out.println(actionMap);

        for(Map.Entry<String,List<Row>> entry: actionMap.entrySet()) {
            List<Row> actions1 = entry.getValue();
            String tableName1 = entry.getKey();
            Iterator i$ = actions1.iterator();
            while(i$.hasNext()) {
                System.out.println("123");
                System.out.println(i$.next());
            }
        }

    }
    private <T> Map<String,List<T>> addMap(Map<String,List<T>> batchMap,Map<String,List<T>> actionMap) {

        if(actionMap == null) {
            return batchMap;
        }

        for(Map.Entry<String,List<T>> entry : actionMap.entrySet()) {
            String key = entry.getKey();
            List<T> value = entry.getValue();
            if(batchMap.containsKey(key)) {
                List<T> batchValue = batchMap.get(key);
                batchValue.addAll(value);
                batchMap.put(key,batchValue);
            }else {
                batchMap.put(key,value);
            }
        }
        return batchMap;
    }

}
