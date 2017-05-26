package com.asiainfo.ocsearch.cache;

import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/5/19.
 */
public interface ICache {
    public void put(String key,String o) throws Exception;
    public String get(String key)  throws Exception;
    public void remove(String key) throws Exception;

    public void put(String primaryKey,Map<String ,String> kv) throws Exception;
    public Map<String, String> get(String primaryKey, List<String> secondaryKeys)  throws Exception;
}
