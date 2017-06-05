package com.asiainfo.ocsearch.datainput.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.util.Map;

/**
 * Created by Aaron on 17/5/26.
 */
public class ConfigurationManager extends Configured {

    public Configuration getConfiguration() {
        return configuration;
    }

    private Configuration configuration = getConf();

    public ConfigurationManager(Map<String,String> conf)
    {
        initialConf(conf);
    }


    private void initialConf(Map<String,String> inputConf)
    {
        for (Map.Entry<String,String> entry: inputConf.entrySet())
        {
            configuration.set(entry.getKey(),entry.getValue());
        }
    }

}
