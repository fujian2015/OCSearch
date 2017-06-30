package com.asiainfo.ocsearch.constants;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Created by mac on 2017/3/27.
 */
public class OCSearchEnv {

    private static Properties prop = new Properties();

    public static void setUp(Properties p) {
        p.entrySet().forEach(entry->{
            String k = String.valueOf(entry.getKey());
            if (StringUtils.startsWith(k, "global."))
                prop.setProperty(k.substring(7), (String) entry.getValue());
        });
    }

    public static String getEnvValue(String key) {
        return prop.getProperty(key);
    }

    public static String getEnvValue(String key, String defualt) {
        return prop.getProperty(key, defualt);
    }

    public static void setEnvValue(String key, String value) {
        prop.setProperty(key, value);
    }

}
