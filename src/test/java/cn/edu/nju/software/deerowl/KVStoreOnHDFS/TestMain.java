package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.DBManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.FileReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.LogHelper;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rale on 11/12/17.
 */
public class TestMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

//        Configuration configuration = new Configuration();
//        configuration.set("fs.default.name", "hdfs://localhost:8020");
//        DBManager dbManager = new DBManager(configuration);
//        for (int i = 0 ; i<100 ; i++){
//            Map<String, String> value = new HashMap<>();
//            value.put("hello", "world");
//            String key = "test" + i;
//            dbManager.write(key, value, true);
//        }
//
//        LogHelper logHelper = new LogHelper(TestConfig.MAX_LOG);
//        System.out.println(logHelper.readLog().size());

        //测试生成key value
        TestMain test = new TestMain();
        test.autoGenerate();

//        FileReader fileReader = new FileReader(configuration, new Path("/data/3b16b45a-5d05-4068-83b0-ad5c2f153a31.seq"));
//        System.out.println(fileReader.readAll().size());
    }

    public Map<String, Map<String, String>> autoGenerate(){
        Map<String, Map<String, String>> map = new HashMap<>();
        String key = "rale";

        for(int i=0;i<100;i++){
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
