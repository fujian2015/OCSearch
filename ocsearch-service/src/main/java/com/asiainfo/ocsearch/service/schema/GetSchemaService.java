package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/6/12.
 */
public class GetSchemaService extends OCSearchService {
    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {
        try {
            String type = request.get("type").asText();
            String name = request.get("name").asText();

            Schema schema;

            if (StringUtils.equals(type.toLowerCase(), "schema"))
                schema = MetaDataHelperManager.getInstance().getSchemaBySchema(name);
            else if (StringUtils.equals(type.toLowerCase(), "table"))
                schema = MetaDataHelperManager.getInstance().getSchemaByTable(name);
            else
                throw new ServiceException("the type must be 'schema' or 'table'", ErrorCode.PARSE_ERROR);

            if (schema == null)
                throw new ServiceException("the schema does not exist", ErrorCode.SCHEMA_NOT_EXIST);

            successResult.put("schema", schema.toJsonNode());

            return successResult.toString().getBytes(Constants.DEFUAT_CHARSET);

        } catch (ServiceException e) {
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            if (successResult.has("schema"))
                successResult.remove("schema");
        }
    }
}
