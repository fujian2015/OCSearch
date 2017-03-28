package com.asiainfo.ocsearch.common;

import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;

import java.util.Properties;

/**
 * Created by mac on 2017/3/27.
 */
public class OCSearchEnv {

    static  final String ENV_FILE="ocsearch_env.properties";

    private static Properties prop= PropertiesLoadUtil.loadProFile(ENV_FILE);

    public  static  String getEnvValue(String key){
        return prop.getProperty(key);
    }
    public  static  String getEnvValue(String key,String defualt){
        return prop.getProperty(key,defualt);
    }
}
