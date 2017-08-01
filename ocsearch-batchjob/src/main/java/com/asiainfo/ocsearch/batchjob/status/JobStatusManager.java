package com.asiainfo.ocsearch.batchjob.status;

public class JobStatusManager {

    JobStatusListener jobStatusListener;

    public void setJobStatusListener(JobStatusListener jobStatusListener) {

        this.jobStatusListener = jobStatusListener;

    }

    public void jobStatusChange(JobStatusResult result) {

        jobStatusListener.doPutCallback(result);

    }

    public void jobStatusQuery(String jobid) {
        jobStatusListener.doGetCallback(jobid);
    }


}
