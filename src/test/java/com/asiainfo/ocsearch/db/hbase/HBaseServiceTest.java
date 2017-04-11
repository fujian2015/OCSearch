package com.asiainfo.ocsearch.db.hbase;

import org.junit.Test;

/**
 * Created by mac on 2017/4/7.
 */
public class HBaseServiceTest {
    @Test
    public void getDefaultRegions() throws Exception {
        for (byte[] bytes : HBaseService.getDefaultRegions(1024)) {

            for (byte b : bytes) {
                System.out.print((b&0xff)+"|");
            }
            System.out.println();
        }
    }

}