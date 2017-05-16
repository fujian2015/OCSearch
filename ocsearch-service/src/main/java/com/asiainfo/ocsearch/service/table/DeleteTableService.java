package com.asiainfo.ocsearch.service.table;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.IndexType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.atomic.table.DeleteHbaseTable;
import com.asiainfo.ocsearch.transaction.atomic.table.DeleteIndexerTable;
import com.asiainfo.ocsearch.transaction.atomic.table.DeleteSolrCollection;
import com.asiainfo.ocsearch.transaction.atomic.table.RemoveTableFromZk;
import com.asiainfo.ocsearch.transaction.internal.TransactionImpl;
import com.asiainfo.ocsearch.transaction.internal.TransactionUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

/**
 * Created by mac on 2017/5/4.
 */
public class DeleteTableService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    Logger log = Logger.getLogger(getClass());

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {
        String uuid = getRequestId();
        try {
            stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());
            String name = request.get("name").asText();

            if (!MetaDataHelperManager.getInstance().hasTable(name)) {
                throw new ServiceException("table " + name + " does not exist!", ErrorCode.TABLE_NOT_EXIST);
            }
            Schema schema = MetaDataHelperManager.getInstance().getSchemaByTable(name);

            Transaction transaction = new TransactionImpl();

//            transaction.add(new RemoveTableFromDb(name));
            transaction.add(new RemoveTableFromZk(name));  //instead db with zookeeper

            IndexType indexType = schema.getIndexType();

            if (indexType == IndexType.HBASE_SOLR_INDEXER)
                transaction.add(new DeleteIndexerTable(name));
            if (indexType != IndexType.HBASE_ONLY)
                transaction.add(new DeleteSolrCollection(name));

            transaction.add(new DeleteHbaseTable(name));

            try {
                transaction.execute();
            } catch (Exception e) {
                TransactionUtil.serialize(uuid+"_table_delete_"+name,transaction,false);
                log.error("delete table " + name + " failure", e);
                throw e;
            }
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }

        return success;
    }
}
