package com.asiainfo.ocsearch.batchjob.status;

import java.util.Map;

public class JobStatusResult {

    public static final String PREPARE = "PREPARE";
    public static final String RUNNING = "RUNNING";
    public static final String SUCCESS = "SUCCESS";
    public static final String FAILED = "FAILED";
    private String jobId;
    private long startTime = 0L;
    private long endTime = 0L;
    private String status = PREPARE;
    private long badlines = 0L;
    private long totallines = 0L;

    JobStatusListener jobStatusListener = null;

    public JobStatusResult(String jobId) {
        this.jobId = jobId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getJobId() {
        return jobId;
    }

    public long getBadlines() {
        return badlines;
    }

    public void setBadlines(long badlines) {
        this.badlines = badlines;
    }

    public long getTotallines() {
        return totallines;
    }

    public void setTotallines(long totallines) {
        this.totallines = totallines;
    }

    public void setJobStatusListener(JobStatusListener jobStatusListener) {
        this.jobStatusListener = jobStatusListener;
    }

    public void startJob(long startTime) {
        this.startTime = startTime;
        this.status = RUNNING;
        this.jobStatusListener.doPutCallback(this);
    }

    public void finishJob(long endTime) {
        this.endTime = endTime;
        this.status = SUCCESS;
        this.jobStatusListener.doPutCallback(this);
    }

    public void failJob(long endTime) {
        this.endTime = endTime;
        this.status = FAILED;
        this.jobStatusListener.doPutCallback(this);
    }

    public Map<String,String> getJobStatus() {
        return this.jobStatusListener.doGetCallback(jobId);
    }



}
