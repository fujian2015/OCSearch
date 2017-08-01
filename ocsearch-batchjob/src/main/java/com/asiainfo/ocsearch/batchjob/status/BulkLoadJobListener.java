package com.asiainfo.ocsearch.batchjob.status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkLoadJobListener implements JobStatusListener {

    JobStatusCache jobStatusCache = (JobStatusCache) JobCacheManager.getCache();
    String badlineColumnName = "BADNUMS";
    String totalLineColumnName = "TOTALNUMS";
    List<String> columnsList = new ArrayList<>();

    public BulkLoadJobListener() {
        columnsList.add(JobStatusCache.statusColumn);
        columnsList.add(JobStatusCache.startTimeColumn);
        columnsList.add(JobStatusCache.endTimeColumn);
        columnsList.add(badlineColumnName);
        columnsList.add(totalLineColumnName);
    }

    @Override
    public boolean doPutCallback(JobStatusResult result) {

        String jobid = result.getJobId();
        Map<String, String> columnsField = new HashMap<>();
        columnsField.put(JobStatusCache.statusColumn,result.getStatus());
        columnsField.put(JobStatusCache.startTimeColumn,String.valueOf(result.getStartTime()));
        columnsField.put(JobStatusCache.endTimeColumn,String.valueOf(result.getEndTime()));
        columnsField.put(badlineColumnName,String.valueOf(result.getBadlines()));
        columnsField.put(totalLineColumnName,String.valueOf(result.getTotallines()));
        try {
            jobStatusCache.put(jobid,columnsField);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }

    @Override
    public Map<String,String> doGetCallback(String jobid) {
        try {
            Map<String,String> result = this.jobStatusCache.get(jobid,columnsList);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
