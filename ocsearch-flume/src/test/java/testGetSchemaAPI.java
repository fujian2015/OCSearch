import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.flume.util.HttpRestFulClient;
import com.asiainfo.ocsearch.meta.Schema;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;

/**
 * Created by Aaron on 17/7/13.
 */
public class testGetSchemaAPI {

    @Test
    public void testGetRestful() {
        String targetUrl = "http://10.1.236.66:28080/ocsearch-service/schema/get?type=schema&name=schemaYidong";
        JsonNode jsonNode = HttpRestFulClient.getRequest(targetUrl);
        jsonNode = jsonNode.get("schema");
        Schema schema = null;
        try {
            schema = new Schema(jsonNode);
            System.out.println(schema);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }


}
