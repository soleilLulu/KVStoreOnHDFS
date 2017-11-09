package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.helium.kvstore.processor.Processor;

import java.util.Map;

/**
 * Created by rale on 11/8/17.
 */
public class MyProcessor implements Processor{
    @Override
    public Map<String, String> get(String s) {
        return null;
    }

    @Override
    public boolean put(String s, Map<String, String> map) {
        return false;
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        return false;
    }

    @Override
    public byte[] process(byte[] bytes) {
        return new byte[0];
    }
}
