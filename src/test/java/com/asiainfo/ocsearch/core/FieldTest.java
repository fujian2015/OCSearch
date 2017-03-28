package com.asiainfo.ocsearch.core;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mac on 2017/3/23.
 */
public class FieldTest {

    Field field;

    @Before
    public void setUp() throws Exception {
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.put("name", "title");
        jsonNode.put("type", "int");

        jsonNode.put("isIndexed", "true");
        jsonNode.put("isContent", "false");

        field = new Field(jsonNode);

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void isContent() throws Exception {
        System.out.println(field.contented);
    }

}