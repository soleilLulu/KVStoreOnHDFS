package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by rale on 11/9/17.
 * 二级索引写出流
 */
public class RangeWriter {

    private String RANGE_DIR;
    private Configuration configuration;
    private FileSystem fileSystem;

    public RangeWriter(Configuration configuration, String dir) throws IOException {
        this.configuration = configuration;
        this.RANGE_DIR = dir;
        fileSystem = FileSystem.get(configuration);
        mkdirs(new Path(RANGE_DIR));
    }

    public void write(Range range){
        write(range, generatePath(range));
    }


    private void write(Range range, Path path){
        FSDataOutputStream dataOutput = null;
        try {
            dataOutput = fileSystem.create(path);
            range.write(dataOutput);
            dataOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 新建文件夹
     * @param path
     * @throws IOException
     */
    private void mkdirs(Path path) throws IOException {
        if (!fileSystem.exists(path)){
            fileSystem.mkdirs(path);
        }
    }


    private Path generatePath(Range range){
        return new Path(RANGE_DIR + UUID.randomUUID());
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
//        RangeWriter rangeWriter = new RangeWriter(configuration);
//        rangeWriter.write(new Range("1", "10", "what"), new Path("/range/test"));
//        rangeWriter.write(new Range("10","100","what2"), new Path("/range/test"));


    }

}
