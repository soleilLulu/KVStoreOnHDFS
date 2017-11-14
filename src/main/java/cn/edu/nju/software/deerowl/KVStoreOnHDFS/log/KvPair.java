package cn.edu.nju.software.deerowl.KVStoreOnHDFS.log;

import java.io.Serializable;
import java.util.Map;

public class KvPair implements Serializable{
    private static final long serialVersionUID = 5224759965392034957L;
    private String key;
    private Map<String,String> value;

    public KvPair(String key, Map<String, String> value){
        this.setKey(key);
        this.setValue(value);
    }

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

    @Override
    public String toString(){
        return key + value;
    }
}
