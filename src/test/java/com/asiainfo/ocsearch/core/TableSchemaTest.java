package com.asiainfo.ocsearch.core;

import com.asiainfo.ocsearch.CommonUtils;
import org.dom4j.Element;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public class TableSchemaTest {
    @Test
    public void getKeyFields() throws Exception {
        System.out.println(tableSchema.getKeyFields());
    }

    TableSchema tableSchema;
    @Before
    public void setUp() throws Exception {

        this.tableSchema = new TableSchema(CommonUtils.getRquestDemo());
    }


    @Test
    public void getTableFields() throws Exception {

        System.out.println(tableSchema.getTableFields());
    }

    @Test
    public void getSchemaFields() throws Exception {

        System.out.println(tableSchema.getSchemaFields());
    }

    @Test
    public void getBaseFields() throws Exception {

            System.out.println(tableSchema.getBaseFields());

    }

    @Test
    public void getQueryFields() throws Exception {


        System.out.println(tableSchema.getQueryFields());
    }

    @Test
    public void getSolrFields() throws Exception {

        Iterator var4 = tableSchema.getSolrFields().iterator();

        while(var4.hasNext()) {
            Element element = (Element)var4.next();
            System.out.println(element.asXML());
        }
    }
}
