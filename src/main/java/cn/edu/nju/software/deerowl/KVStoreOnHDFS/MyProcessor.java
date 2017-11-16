package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.DBManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * Created by rale on 11/8/17.
 */
public class MyProcessor implements Processor{
    DBManager dbManager;

    public MyProcessor(){
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", KvStoreConfig.getHdfsUrl());
        System.out.println(TestConfig.HDFS_URL);

        try {
            dbManager = new DBManager(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public Map<String, String> get(String s) {
        if (s==null || s.equals("")) return null;
        try {
            return dbManager.read(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean put(String s, Map<String, String> map) {
        try {
            dbManager.write(s, map, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()){
            if (!put(entry.getKey(), entry.getValue())){
                return false;
            }
        }
        return true;
    }

    @Override
    public byte[] process(byte[] bytes) {
        return new byte[0];
    }
}
