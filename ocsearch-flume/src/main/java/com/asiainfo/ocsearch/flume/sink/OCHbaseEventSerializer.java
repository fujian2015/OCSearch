package com.asiainfo.ocsearch.flume.sink;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

import java.util.List;
import java.util.Map;

/**
 * Created by Aaron on 17/6/27.
 */
public interface OCHbaseEventSerializer extends Configurable, ConfigurableComponent {
    void initialize(Event var1);

    Map<String,List<Row>> getActions();

    Map<String,List<Increment>> getIncrements();

    void close();
}
