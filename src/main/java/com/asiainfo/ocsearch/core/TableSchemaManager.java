package com.asiainfo.ocsearch.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mac on 2017/4/1.
 */
public class TableSchemaManager {

    private static Map<String, TableSchema> tableConfigs = new ConcurrentHashMap<String, TableSchema>();

    static {
        initialManager();
    }

    private static void initialManager() {

    }

    public static boolean existsConfig(String configName) {

        return tableConfigs.containsKey(configName);
    }


}
