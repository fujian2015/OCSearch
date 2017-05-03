package com.asiainfo.ocsearch.env;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import com.asiainfo.ocsearch.datasource.mysql.DataSourceProvider;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.service.schema.AddSchemaService;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Properties;

/**
 * Created by mac on 2017/4/19.
 */
public class StartOcsearch {
    public static void  startService() throws Exception {
        Properties properties=PropertiesLoadUtil.loadProFile("ocsearch.properties");

        DataSourceProvider.setUp(properties);

        OCSearchEnv.setUp(properties);

//        SolrServer.setUp(properties);

//        Configuration configuration = HBaseConfiguration.create();
//        HbaseServiceManager.setup(configuration);

    }
    public static void main(String []args) throws Exception {
        startService();



        JsonNode jsonNode=new ObjectMapper().readTree("{\"name\":\"testSchema\",\"rowkey_expression\":\"\",\"table_expression\":\"\",\"content_field\":{\"name\":\"_root_\",\"type\":\"text\"},\"query_fields\":[{\"name\":\"title\",\"weight\":10},{\"name\":\"content\",\"weight\":20}],\"fields\":[{\"name\":\"length\",\"indexed\":true,\"index_contented\":true,\"index_stored\":false,\"index_type\":\"int\",\"hbase_column\":\"length\",\"hbase_family\":\"B\",\"store_type\":\"INT\"},{\"name\":\"title\",\"indexed\":true,\"index_contented\":true,\"index_stored\":true,\"index_type\":\"text_gl\",\"hbase_column\":\"title\",\"hbase_family\":\"B\",\"store_type\":\"STRING\"},{\"name\":\"content\",\"indexed\":false,\"index_contented\":false,\"index_stored\":false,\"index_type\":\"text_gl\",\"hbase_column\":\"content\",\"hbase_family\":\"B\",\"store_type\":\"STRING\"}]}");
        try {
            new AddSchemaService().doService(jsonNode);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }

}
