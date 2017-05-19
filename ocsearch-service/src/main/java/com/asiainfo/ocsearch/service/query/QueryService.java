package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mac on 2017/5/18.
 */
public abstract class QueryService extends OCSearchService {

    public Set<String> generateReturnFields(Schema schema, ArrayNode returnNodes) {
        Set<String> returnFields;
        if (returnNodes == null || returnNodes.size() == 0) {
            returnFields = schema.getFields().keySet();
        } else {
            returnFields = new HashSet<>();
            for (JsonNode node : returnNodes) {
                returnFields.add(node.asText());
            }
        }
        return returnFields;
    }

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {
        try {

            successResult.put("data", query(request));

            return successResult.toString().getBytes();

        } catch (ServiceException e) {
            throw e;
        } finally {
            successResult.remove("data");
        }
    }

    protected abstract JsonNode query(JsonNode request) throws ServiceException;

}
