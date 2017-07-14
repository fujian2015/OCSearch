package com.asiainfo.ocsearch.flume.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aaron on 17/7/7.
 */
public class PayloadConverter {

    private String inputFormat;
    private String csvSeparator;
    private Map<String,Integer> fieldSequence;


    public PayloadConverter(String inputFormat, String separator, Map<String,Integer> fieldSequence) {

        this.csvSeparator = separator;
        this.inputFormat = inputFormat;
        this.fieldSequence = fieldSequence;

    }

    public Map<String,Object> prepareData(byte[] payload) {

        switch (this.inputFormat)
        {
            case "json":
                return Json2Map(payload);
            case "csv":
                return Csv2Map(payload);
            default:
                return null;

        }
    }

    private Map<String,Object> Csv2Map(byte[] payload) {

        Map<String,Object> result = new HashMap<>();
        String dataStr = new String(payload);
        String[] datas = dataStr.split(this.csvSeparator);
        for(Map.Entry<String,Integer> entry:this.fieldSequence.entrySet()) {
            result.put(entry.getKey(),datas[entry.getValue()-1]);
        }
        return result;

    }

    private Map<String,Object> Json2Map(byte[] payload) {

        Map<String,Object> result = new HashMap<>();
        String jsonStr = new String(payload);
        JsonNode jsonNode = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            jsonNode = mapper.readTree(jsonStr);
            result = mapper.convertValue(jsonNode,Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;

    }


}
