package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by mac on 2017/5/9.
 */
public class DeleteIndexerConfig implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    String schema;

    public DeleteIndexerConfig(String schema) {
        this.schema = schema;
    }

    @Override
    public boolean execute() {

        log.info("delete indexer config "+schema+" start!");
        String path = ConfigUtil.getIndexerConfigPath(schema);

        File dir = new File(path);

        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            throw new RuntimeException("delete indexer dir error!", e);
        }
        log.info("delete indexer config "+schema+" success!");
        return true;
    }

    @Override
    public boolean recovery() {
        return false;
    }

    @Override
    public boolean canExecute() {
        return false;
    }
}
