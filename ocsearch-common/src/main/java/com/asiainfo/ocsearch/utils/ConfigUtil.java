package com.asiainfo.ocsearch.utils;


import com.asiainfo.ocsearch.constants.OCSearchEnv;

import java.io.File;

/**
 * Created by mac on 2017/3/30.
 */
public class ConfigUtil {
    public static String getSolrConfigPath(String config) {
        return OCSearchEnv.getEnvValue("work_dir", "work") + "/solr/" + config + "/";
    }

    public static String getIndexerConfigPath(String config) {
        return OCSearchEnv.getEnvValue("work_dir", "work") + "/indexer/" + config + "/";
    }

    public static boolean configExists(String config) {

        return new File(getIndexerConfigPath(config)).exists();
    }
}
