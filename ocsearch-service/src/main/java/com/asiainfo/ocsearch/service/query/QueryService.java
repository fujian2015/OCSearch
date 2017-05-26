package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mac on 2017/5/18.
 */
public abstract class QueryService extends OCSearchService {

    public Set<String> generateReturnFields(Schema schema, ArrayNode returnNodes) throws ServiceException {
        Set<String> returnFields;
        if (returnNodes == null || returnNodes.size() == 0) {
            returnFields = schema.getFields().keySet();
        } else {
            returnFields = new HashSet<>();
            for (JsonNode node : returnNodes) {
                String name = node.asText();
                if (name.equals("id") || name.equals("_table_"))
                    continue;
                else if (!schema.getFields().containsKey(name))
                    throw new ServiceException("in valid return field:" + name, ErrorCode.PARSE_ERROR);
                else
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

    protected String generateCacheKey(String rowKey, String condition, Set<String> tables) {
        return new StringBuilder(rowKey).append("|").append(condition).append("|").append(StringUtils.join(tables, ",")).toString();
    }

    protected abstract JsonNode query(JsonNode request) throws ServiceException;

}
