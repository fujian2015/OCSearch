package com.asiainfo.ocsearch.utils;

import com.asiainfo.ocsearch.common.OCSearchEnv;

/**
 * Created by mac on 2017/3/30.
 */
public class ConfigUtil {
    public static String getSolrConfigPath(String config){
        return OCSearchEnv.getEnvValue("work_dir", "work") + "/solr/" + config + "/";
    }

    public static String getIndexerConfigPath(String config){
        return OCSearchEnv.getEnvValue("work_dir", "work") + "/indexer/" + config + "/";
    }

}
