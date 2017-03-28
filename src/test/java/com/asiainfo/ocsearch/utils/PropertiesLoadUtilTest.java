package com.asiainfo.ocsearch.utils;

import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PropertiesLoadUtilTest {
    public PropertiesLoadUtilTest() {
    }

    @Test
    public void loadFile() throws Exception {
        System.out.println(PropertiesLoadUtil.loadFile("managed-schema"));
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void loadProFile() throws Exception {
        Assert.assertEquals(PropertiesLoadUtil.loadProFile("solr_bak.properties"), (Object)null);
        Assert.assertNotEquals(PropertiesLoadUtil.loadProFile("solr.properties"), (Object)null);
    }

    @Test
    public void loadXmlFile() throws Exception {
        Assert.assertEquals(PropertiesLoadUtil.loadXmlFile("a"), (Object)null);
        Assert.assertNotEquals(PropertiesLoadUtil.loadXmlFile("hbase-site.xml").get("hbase.rootdir"), (Object)null);
    }
}
