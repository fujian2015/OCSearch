package com.asiainfo.ocsearch.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

import java.util.List;
import java.util.Map;

/**
 * Created by Aaron on 17/6/30.
 */
public class FileHbaseEventSerializer implements OCHbaseEventSerializer {
    @Override
    public void initialize(Event var1) {

    }

    @Override
    public Map<String, List<Row>> getActions() {
        return null;
    }

    @Override
    public Map<String, List<Increment>> getIncrements() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
