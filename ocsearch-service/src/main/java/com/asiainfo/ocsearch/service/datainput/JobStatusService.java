package com.asiainfo.ocsearch.service.datainput;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.service.OCSearchService;
import com.asiainfo.ocsearch.utils.HttpRestFulClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.UnsupportedEncodingException;

/**
 * Created by Aaron on 17/7/26.
 */
public class JobStatusService extends OCSearchService {
    Logger stateLog = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        String uuid = getRequestId();
        stateLog.info("start request " + uuid + " at " + System.currentTimeMillis());

        String state = null;
        String finalStatus = null;

        Configuration configuration = new Configuration();

        String jobid = request.get("jobid").asText();

        if(jobid == null) {
            throw new ServiceException("JOBID is null", ErrorCode.RUNTIME_ERROR);
        }

        String applicationID = jobid.replaceFirst("job","application");

        String rmAddress = configuration.get("yarn.resourcemanager.webapp.address",null);

        if(rmAddress == null) {
            throw new ServiceException("ResourceManager Address is null", ErrorCode.RUNTIME_ERROR);
        }
        String url = rmAddress + "/ws/v1/cluster/apps/" + applicationID;
        try {
            JsonNode jsonNode = HttpRestFulClient.getRequest(url);
            if(jsonNode == null) {
                throw new ServiceException("JOBID is wrong", ErrorCode.RUNTIME_ERROR);
            }
            jsonNode = jsonNode.get("app");
            state = jsonNode.get("state").asText();
            finalStatus = jsonNode.get("finalStatus").asText();
            ObjectNode successResult = getSuccessResult();
            successResult.put("state",state);
            successResult.put("finalStatus",finalStatus);
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
