package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.db.solr.SolrServer;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by mac on 2017/3/30.
 */
public class UploadConfig  implements AtomicOperation,Serializable{

    String  config;


    public  UploadConfig(String config){

        this.config =config;

    }

    @Override
    public boolean execute() {

        String configPath= ConfigUtil.getSolrConfigPath(config);

        try {
            Path path = Paths.get(configPath);
            SolrServer.getInstance().uploadConfig(path,this.config);
        } catch (IOException e) {
            throw new RuntimeException("upload config "+configPath +" failure!",e);
        }

        return true;
    }

    @Override
    public boolean recovery() {

        try {
            if(SolrServer.getInstance().existConfig(this.config))
                SolrServer.getInstance().deleteConfig(config);
        } catch (IOException e) {
            throw new RuntimeException("delete config "+config +" failure!",e);
        }
        return true;
    }

    @Override
    public boolean canExecute() {

        try {
            return !SolrServer.getInstance().existConfig(this.config);
        } catch (IOException e) {
            throw new RuntimeException("check config "+config +" failure!",e);
        }
    }
}
