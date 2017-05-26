package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelper;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.schema.DeleteSolrConfig;
import com.asiainfo.ocsearch.transaction.atomic.schema.RemoveSchemaFromZk;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/5/9.
 */
public class DeleteSchemaService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();

        try {

            stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

            String name = request.get("name").asText();

            MetaDataHelper metaDataHelper = MetaDataHelperManager.getInstance();
            Schema schema = metaDataHelper.getSchemaBySchema(name);

            if (schema == null) {
                throw new ServiceException(String.format("schema :%s  does not exist.", name), ErrorCode.SCHEMA_NOT_EXIST);
            }

            if (metaDataHelper.schemaInUse(name) == true) {
                throw new ServiceException(String.format("schema :%s  is in use.", name), ErrorCode.SCHEMA_IN_USE);
            }

            Transaction transaction = new TransactionImpl();

//            transaction.add(new RemoveSchemaFromDb(name));
            transaction.add(new RemoveSchemaFromZk(name)); //instead db with zookeeper

            if (schema.getIndexType() != IndexType.HBASE_ONLY) {
//                transaction.add(new DeleteIndexerConfig(name));
                transaction.add(new DeleteSolrConfig(name));
            }
            try {
                transaction.execute();
            } catch (Exception e) {
                log.error("delete schema " + name + " failure", e);
                TransactionUtil.serialize(uuid + "_schema_delete_" + name, transaction, false);
                throw e;
            }
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }
        return success;
    }
}
