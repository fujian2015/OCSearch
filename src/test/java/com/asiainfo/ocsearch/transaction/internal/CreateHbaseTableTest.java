package com.asiainfo.ocsearch.transaction.internal;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mac on 2017/4/7.
 */
public class CreateHbaseTableTest {

    CreateHbaseTable createHbaseTable;

    @Before
    public void setUp() throws Exception {
        Set<byte[]> families = new HashSet<>();
        families.add(Bytes.toBytes("B"));
        families.add(Bytes.toBytes("C"));
        this.createHbaseTable = new CreateHbaseTable(16, "testTbale", families);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void execute() throws Exception {

    }

    @Test
    public void recovery() throws Exception {

    }

    @Test
    public void canExecute() throws Exception {
        Assert.assertEquals(true,createHbaseTable.canExecute());
    }

}