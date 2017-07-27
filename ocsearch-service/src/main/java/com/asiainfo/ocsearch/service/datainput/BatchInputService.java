package com.asiainfo.ocsearch.service.datainput;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.datainput.batch.BatchJobClient;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by Aaron on 17/6/6.
 */
public class BatchInputService extends OCSearchService {

    Logger stateLog = Logger.getLogger("state");

    @Override
    protected byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

        try {
            String tableName = request.get("tablename").asText();
            String inputPath = request.get("inputpath").asText();
            String outputPath = request.get("outputpath").asText();
            String dataSeparator = request.get("dataseparator").asText();
            JsonNode fieldSequenceNode = request.get("fieldsequence");
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Integer> fieldSequence = mapper.convertValue(fieldSequenceNode, Map.class);
            Schema schema= MetaDataHelperManager.getInstance().getSchemaByTable(tableName);
            if (schema == null) {
                throw new ServiceException("schema for table : " + tableName + " does not exist!", ErrorCode.PARSE_ERROR);
            }

            String jobId= BatchJobClient.submitJob
                    (inputPath,outputPath,tableName,dataSeparator,fieldSequence,schema);
            boolean submitSuccess = !jobId.equals("-1");
            if(!submitSuccess) {
                throw new ServiceException(String.format("submit job failure."), ErrorCode.RUNTIME_ERROR);
            }
            ObjectNode successResult = getSuccessResult();
            successResult.put("JOBID",jobId);
            return successResult.toString().getBytes(Constants.DEFUAT_CHARSET);

        }catch (ServiceException e){
            stateLog.warn(e);
            throw e;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            stateLog.info("end request " + uuid + " at " + System.currentTimeMillis());
        }
        return success;
    }
}
