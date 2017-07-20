package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Collection;

/**
 * Created by mac on 2017/7/11.
 */
public class ListSchemaService extends OCSearchService {
    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {
        try {

            Collection<Schema> schemas = MetaDataHelperManager.getInstance().getSchemas();

            ArrayNode schemaNodes = JsonNodeFactory.instance.arrayNode();

            schemas.forEach(schema -> schemaNodes.add(schema.toJsonNode()));

            ObjectNode successResult = getSuccessResult();
            successResult.put("schemas", schemaNodes);

            return successResult.toString().getBytes(Constants.DEFUAT_CHARSET);

        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}
