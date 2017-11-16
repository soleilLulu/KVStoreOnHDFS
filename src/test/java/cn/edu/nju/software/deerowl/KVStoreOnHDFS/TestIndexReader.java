package cn.edu.nju.software.deerowl.KVStoreOnHDFS;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.IndexReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by rale on 11/14/17.
 */
public class TestIndexReader {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        IndexReader indexReader = new IndexReader(configuration, new Path("/data/c65a4a2c-80f6-4210-99c4-103613fec719.idx"));
        System.out.println(indexReader.readAll());
        indexReader.close();
        System.out.println(indexReader.getPosition(new Key("test70")));
    }
}
