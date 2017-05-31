package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
import com.asiainfo.ocsearch.query.FileID;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by mac on 2017/5/31.
 */
public class FileGetServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
//        Put put = new Put("test1".getBytes("UTF-8"));
//        put.addColumn("C".getBytes(), "picture".getBytes(), FileUtils.readFileToByteArray(new File("/Users/mac/Documents/timg.jpg")));
//        put.addColumn("C".getBytes(), "FILES".getBytes(), "picture".getBytes());
//
//        HbaseServiceManager.getInstance().getAdminService().execute("file__table", table -> {
//            table.put(put);
//            return true;
//        });
        FileID fileID = new FileID("file__table", "picture:picture", "test1");
        System.out.println(fileID);
        ObjectNode o = JsonNodeFactory.instance.objectNode();
        o.put("id", "eyJ0IjoiZmlsZV9fdGFibGUiLCJmIjoicGljdHVyZTpwaWN0dXJlIiwiciI6InRlc3QxIn0=");

        FileUtils.writeByteArrayToFile(new File("/Users/mac/Documents/timg2.jpg"), new FileGetService().doService(o));
    }

}