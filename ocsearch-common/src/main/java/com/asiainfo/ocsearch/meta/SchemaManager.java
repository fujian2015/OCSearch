package com.asiainfo.ocsearch.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mac on 2017/4/1.
 */
public class SchemaManager {

    private static Map<String, Schema> tableConfigs = new ConcurrentHashMap<String, Schema>();


    public static boolean existsConfig(String configName) {

        return tableConfigs.containsKey(configName);
    }

    /**
     * overwrite schema
     * @param configName
     * @param schema
     */
    public static synchronized  void addSchema(String configName,Schema schema) {
        tableConfigs.put(configName,schema);
    }

    public static synchronized void removeSchema(String configName){
        tableConfigs.remove(configName);
    }

    public static synchronized Schema getSchema(String configName){
        return tableConfigs.get(configName);
    }


}
