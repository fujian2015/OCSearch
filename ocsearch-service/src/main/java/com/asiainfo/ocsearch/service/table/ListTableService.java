package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Set;

/**
 * Created by mac on 2017/6/27.
 */
public class ListTableService extends OCSearchService {
    @Override
    public byte[] doService(JsonNode request) throws ServiceException {
        try {

            Set<String> tables = MetaDataHelperManager.getInstance().getTables();

            ArrayNode tableNodes = JsonNodeFactory.instance.arrayNode();

            tables.forEach(table -> tableNodes.add(table));

            ObjectNode successResult = getSuccessResult();
            successResult.put("tables", tableNodes);

            return successResult.toString().getBytes(Constants.DEFUAT_CHARSET);

        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}
