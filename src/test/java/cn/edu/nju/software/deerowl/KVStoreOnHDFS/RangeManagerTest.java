package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.Range;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.RangeManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.conf.Configuration;

import java.util.List;


/**
 * Created by rale on 11/11/17.
 */
public class RangeManagerTest {

    public static void main(String[] args){
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        RangeManager rangeManager = new RangeManager(configuration);
        rangeManager.write(new Range("a", "e","/usr/local"));
        rangeManager.write(new Range("10", "11", "/usr/opt"));
        List<Range> rangeList = rangeManager.findRangeContainsKey(new Key("b"));
        for (Range range : rangeList){
            System.out.println(range);
        }

    }
}
