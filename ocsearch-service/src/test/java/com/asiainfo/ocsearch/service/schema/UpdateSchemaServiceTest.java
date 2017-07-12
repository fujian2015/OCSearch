package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.listener.SystemListener;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Created by mac on 2017/7/5.
 */
public class UpdateSchemaServiceTest {
    @Test
    public void testDoService() throws Exception {
        new SystemListener().initAll();
        JsonNode jsonNode = new ObjectMapper().readTree("{\n" +
                "    \"command\": \"update_field\",\n" +
                "    \"table\": \"SITE\",\n" +
                "    \"field\": {\n" +
                "        \"name\": \"PHONENUM\",\n" +
                "        \"indexed\": true,\n" +
                "        \"index_stored\": false,\n" +
                "        \"index_type\": \"string\",\n" +
                "       \"store_type\": \"STRING\"" +
                "    }\n" +
                "}");
        System.out.println(jsonNode);

        System.out.println(new ObjectMapper().readTree(new UpdateSchemaService().doService(jsonNode)));

        Thread.sleep(1000 * 1);

        System.out.println(MetaDataHelperManager.getInstance().getSchemaByTable("SITE").getFields().get("PHONENUM").toJsonNode());

    }

}