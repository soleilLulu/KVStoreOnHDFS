package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.RangeManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by rale on 11/14/17.
 */
public class TestRange {

    public static void main(String[] args){
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        RangeManager rangeManager = new RangeManager(configuration);
        System.out.println(rangeManager.findRangeContainsKey(new Key("rale10")));
    }
}
