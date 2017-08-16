package com.asiainfo.ocsearch.service.batch;

import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.Test;


/**
 * Created by Aaron on 17/7/26.
 */
public class TestJobStatusService {

    @Test
    public void testDoService() {

        ObjectNode jsonNode = new ObjectMapper().createObjectNode();

        jsonNode.put("jobid","job_1500975519630_0003");

        try {
            byte[] bytes = new JobStatusService().doService(jsonNode);
            System.out.println(new String(bytes));
        } catch (ServiceException e) {
            e.printStackTrace();
        }

    }

}
