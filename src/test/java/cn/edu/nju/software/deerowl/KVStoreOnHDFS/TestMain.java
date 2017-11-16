package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.DBManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.FileReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.LogHelper;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by rale on 11/12/17.
 */
public class TestMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        DBManager dbManager = new DBManager(configuration);

        TreeMap<String, Map<String, String >> map = new TreeMap<>(autoGenerate(321));
//        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()){
//            dbManager.write(entry.getKey(), entry.getValue(), true);
//        }

        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()){
            System.out.println(entry.getKey() + " : " + dbManager.read(entry.getKey()));
        }
//        dbManager.read("rale1");


    }

    public static Map<String, Map<String, String>> autoGenerate(int count){
        Map<String, Map<String, String>> map = new HashMap<>();
        String key = "rale";

        for(int i=0;i<count;i++){
            String temp = key+i;
            Map<String,String> value = new HashMap<>();
            value.put("age" + i,"18");
            value.put("love" + i,"mayday");
            map.put(temp,value);
            //System.out.println(temp);
        }

        //System.out.println(map.size());
        return  map;
    }
}
