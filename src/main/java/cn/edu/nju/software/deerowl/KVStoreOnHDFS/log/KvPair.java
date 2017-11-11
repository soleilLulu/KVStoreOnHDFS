package cn.edu.nju.software.deerowl.KVStoreOnHDFS.log;

import java.io.Serializable;
import java.util.Map;

public class KvPair implements Serializable{
    private String key;
    private Map<String,String> value;


    public Map<String, String> getValue() {
        return value;
    }

    public void setValue(Map<String, String> value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
