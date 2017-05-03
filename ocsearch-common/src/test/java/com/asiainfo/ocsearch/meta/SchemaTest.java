package com.asiainfo.ocsearch.meta;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/4/19.
 */
public class SchemaTest {
    @Test
    public void testToString() throws Exception {
        JsonNode jsonNode=new ObjectMapper().readTree("{\"name\":\"testSchema\",\"rowkey_expression\":\"\",\"table_expression\":\"\",\"content_field\":{\"name\":\"_root_\",\"type\":\"text\"},\"query_fields\":[{\"name\":\"title\",\"weight\":10},{\"name\":\"content\",\"weight\":20}],\"fields\":[{\"name\":\"length\",\"indexed\":true,\"index_contented\":true,\"index_stored\":false,\"index_type\":\"int\",\"hbase_column\":\"length\",\"hbase_family\":\"B\",\"store_type\":\"INT\"},{\"name\":\"title\",\"indexed\":true,\"index_contented\":true,\"index_stored\":true,\"index_type\":\"text_gl\",\"hbase_column\":\"title\",\"hbase_family\":\"B\",\"store_type\":\"STRING\"},{\"name\":\"content\",\"indexed\":false,\"index_contented\":false,\"index_stored\":false,\"index_type\":\"text_gl\",\"hbase_column\":\"content\",\"hbase_family\":\"B\",\"store_type\":\"STRING\"}]}");
        Schema schema=new Schema(jsonNode);


        Schema a= (Schema) schema.clone();
        schema.setContentField(null);
        System.out.println(a);
    }

}