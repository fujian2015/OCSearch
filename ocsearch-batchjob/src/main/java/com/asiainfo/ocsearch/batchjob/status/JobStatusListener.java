package com.asiainfo.ocsearch.batchjob.status;

import java.util.Map;

public interface JobStatusListener {

    public boolean doPutCallback(JobStatusResult result);

    public Map<String,String> doGetCallback(String jobid);

}
