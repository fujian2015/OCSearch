package com.asiainfo.ocsearch.metahelper;

import java.util.Properties;

/**
 * Created by mac on 2017/5/12.
 */
public class MetaDataHelperManager {

    private  static MetaDataHelper metaDataHelper;

    public static  void setUp(Properties properties) throws Exception {
        metaDataHelper=new MetaDataHelper(properties);
    }

    public static MetaDataHelper getInstance(){
        return metaDataHelper;
    }

}
