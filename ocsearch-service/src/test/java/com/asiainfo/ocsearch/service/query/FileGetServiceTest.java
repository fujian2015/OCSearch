package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.listener.SystemListener;
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
//        put.addColumn("C".getBytes(), "pic".getBytes(), FileUtils.readFileToByteArray(new File("/Users/mac/Documents/timg.jpg")));
//        put.addColumn("C".getBytes(), "FILES".getBytes(), "pic".getBytes());
//        put.addColumn("B".getBytes(), "0".getBytes(), "普通文件".getBytes());
//        put.addColumn("B".getBytes(), "1".getBytes(), "20170712 12:12:12".getBytes());
//        put.addColumn("B".getBytes(), "2".getBytes(), "我的而一个测试文件".getBytes());
//
//        HbaseServiceManager.getInstance().getAdminService().execute("FILE__201707", table -> {
//            table.put(put);
//            return true;
//        });
//        FileID fileID = new FileID("file__table", "picture:picture", "test1");
//        System.out.println(fileID);
        ObjectNode o = JsonNodeFactory.instance.objectNode();
        o.put("id", "eyJ0IjoiRklMRV9fMjAxNzA3IiwiZiI6InBpYyIsImUiOiJGSUxFUyIsInIiOiI4NWU0NDViZS1kOTgwLTQ1M2MtOTg0OS0xNGQ5ZjUxYzA2MzUifQ==");

        FileUtils.writeByteArrayToFile(new File("/Users/mac/Documents/timg.jpg"), new FileGetService().doService(o));
    }

}