package com.asiainfo.ocsearch.datainput.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aaron on 17/6/2.
 */
public class ColumnMapConverter {
    private static ColumnMapConverter instance;

    public static ColumnMapConverter getInstance()
    {
        if (instance == null)
            instance = new ColumnMapConverter();
        return instance;
    }

    public String map2String(Map<String,ColumnField> columnFamilyMap) {
        ObjectMapper mapper = new ObjectMapper();
        String result = null;
        try {
            result = mapper.writeValueAsString(columnFamilyMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String,ColumnField> readJson2Map(String json) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String,ColumnField> resultMap = new HashMap<>();
        JavaType javaType = mapper.getTypeFactory().constructParametricType(Map.class,String.class,ColumnField.class);
        try {
            resultMap = mapper.readValue(json,javaType);
        }catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

}
