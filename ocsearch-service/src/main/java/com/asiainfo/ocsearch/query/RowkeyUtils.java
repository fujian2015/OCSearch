package com.asiainfo.ocsearch.query;

import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/8/10.
 */
public class RowkeyUtils {


    private static final String DEFAULT_FORMATTER ="com.ngdata.hbaseindexer.uniquekey.StringUniqueKeyFormatter" ;
    static Map<String, UniqueKeyFormatter> idFormatterHashMap = new HashMap<>();

    public static UniqueKeyFormatter getIdFormatter(String className) {
        if(className ==null)
            className= DEFAULT_FORMATTER;
        if (!idFormatterHashMap.containsKey(className)) {
            try {
                idFormatterHashMap.put(className, (UniqueKeyFormatter) Class.forName(className).newInstance());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return idFormatterHashMap.get(className);
    }

}
