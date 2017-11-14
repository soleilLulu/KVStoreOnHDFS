package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.DBManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.KvPair;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.LogHelper;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by rale on 11/12/17.
 */
public class TestLog {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        LogHelper logHelper = new LogHelper(TestConfig.MAX_LOG, new DBManager(configuration));
        logHelper.writeLog(new KvPair("123", new HashMap<>()));
        logHelper.writeLog(new KvPair("abc", new HashMap<>()));
        for (KvPair temp : logHelper.readLog()){
            System.out.println(temp);
        }
    }
}
