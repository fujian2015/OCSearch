package com.asiainfo.ocsearch.service.internal;

import com.asiainfo.ocsearch.core.TableSchema;
import com.asiainfo.ocsearch.core.TableSchemaManager;
import com.asiainfo.ocsearch.db.hbase.HBaseService;
import com.asiainfo.ocsearch.db.hbase.HbaseServiceManager;
import com.asiainfo.ocsearch.db.solr.SolrServer;
import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.transaction.Transaction;
import com.asiainfo.ocsearch.transaction.TransactionUtil;
import com.asiainfo.ocsearch.transaction.internal.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import java.util.Set;

/**
 * Created by mac on 2017/3/21.
 */
public class CreateTableService extends OCSearchService {

    Logger log = Logger.getLogger(getClass());

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {
        try {
            String uuid = getRequestId();

            TableSchema tableSchema = new TableSchema(request);

            Transaction transaction = new TransactionImpl();

            if (TableSchemaManager.existsConfig(tableSchema.name)) {
                throw new ServiceException(String.format("table :%s  exists.", tableSchema.name), ErrCode.TABLE_EXIST);
            }

            transaction.add(new SaveConfigToDb(tableSchema));

            Set<byte[]> columnFamilies = tableSchema.getHbaseFamilies();

            if (tableSchema.hbaseExist) {

                HBaseService hBaseService = HbaseServiceManager.gethBaseService();
                if (!hBaseService.existsTable(tableSchema.hbaseTbale)) {
                    throw new ServiceException(String.format("hbase table :%s  does not exists.", tableSchema.hbaseTbale),
                            ErrCode.TABLE_NOT_EXISTS);
                }
                if (!hBaseService.existsFamilies(tableSchema.name, columnFamilies)) {
                    throw new ServiceException(String.format("hbase table :%s  families does not match.", tableSchema.hbaseTbale),
                            ErrCode.FAMILY_NOT_MATCH);
                }

            } else if (StringUtils.equals(tableSchema.storeType, "n")) {
                transaction.add(new CreateHbaseTable(tableSchema.hbaseRegions, tableSchema.hbaseTbale, columnFamilies));
            }

            if (tableSchema.solrExist) {
                SolrServer solrServer = SolrServer.getInstance();
                if(!solrServer.existCollection(tableSchema.solrCollection)){
                    throw new ServiceException(String.format("solr collection :%s  does not exists.", tableSchema.solrCollection),
                            ErrCode.TABLE_NOT_EXISTS);
                }
                if(!solrServer.existsFields(tableSchema.solrCollection, tableSchema.getSolrFieldNames())){
                    throw new ServiceException(String.format("solr collection :%s  fields does not match.", tableSchema.solrCollection),
                            ErrCode.TABLE_NOT_EXISTS);
                }

            } else {
                transaction.add(new GenerateSolrConfig(tableSchema));
                transaction.add(new UploadConfig(tableSchema.name));
                if (StringUtils.equals(tableSchema.storeType, "n")) {
                    transaction.add(new CreateSolrCollection(tableSchema.name, tableSchema.solrCollection, tableSchema.solrShards));
                }
            }

            transaction.add(new GenerateIndxerConfig(tableSchema));
            if(StringUtils.equals(tableSchema.storeType, "n"))
                transaction.add(new CreateIndexerTable(tableSchema.name, tableSchema.name, tableSchema.solrCollection, tableSchema.hbaseTbale));

            String transactionName = uuid + "_create_" + tableSchema.name;

            try {
                if (transaction.canExecute()) {
                    TransactionUtil.serialize(transactionName, transaction);
                    transaction.execute();
                    TransactionUtil.deleteTranaction(transactionName);
                } else {
                    throw new ServiceException(String.format("create table :%s  failure.", tableSchema.name), ErrCode.RUNTIME_ERROR);
                }

            } catch (Exception e) {
                log.warn("create table " + tableSchema.name + " failure:", e);
                try {
                    transaction.rollBack();
                } catch (Exception rollBackException) {
                    log.error("roll back creating table " + tableSchema.name + " failure", rollBackException);
                }
                throw e;
            }
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrCode.RUNTIME_ERROR);
        }

        return success;
    }
}
