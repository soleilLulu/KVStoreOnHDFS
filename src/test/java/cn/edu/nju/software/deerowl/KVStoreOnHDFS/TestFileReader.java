package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.FileReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.IndexReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by rale on 11/14/17.
 */
public class TestFileReader {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        IndexReader indexReader = new IndexReader(configuration,new Path("/data/5652bec5-331b-4ca8-964e-4f4137448ca3-1.idx"));
        Key key = new Key("rale15");
        long pos = indexReader.getPosition(key);
        System.out.println(pos);
        indexReader.close();
        FileReader fileReader = new FileReader(configuration, new Path("/data/5652bec5-331b-4ca8-964e-4f4137448ca3-1.seq"));
        System.out.println(fileReader.findFrom(5, key));
    }
}
