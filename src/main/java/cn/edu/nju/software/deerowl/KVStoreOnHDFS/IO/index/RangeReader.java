package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rale on 11/9/17.
 * 获取所有的二级索引
 */
public class RangeReader{

    private String RANGE_DIR;
    private FileSystem fileSystem;

    public RangeReader(Configuration configuration, String dir) throws IOException {
        this.RANGE_DIR = dir;
        fileSystem = FileSystem.get(configuration);
    }

    /**
     * 读取RANGE_DIR目录之下的所有索引
     * @return
     * @throws IOException
     */
    public List<Range> readAll(){
        List<Range> ranges = new ArrayList<>();
        FileStatus[] fileStatus = new FileStatus[0];
        try {
            fileStatus = fileSystem.listStatus(new Path(RANGE_DIR));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path[] listPath = FileUtil.stat2Paths(fileStatus);
        for (Path path : listPath){
            Range range = read(path);
            if (range != null) {
                ranges.add(range);
            }
        }

        return ranges;
    }

    public int numOfRanges(){
        FileStatus[] fileStatus = new FileStatus[0];
        try {
            fileStatus = fileSystem.listStatus(new Path(RANGE_DIR));
            return FileUtil.stat2Paths(fileStatus).length;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 读取单个路径下的索引
     * @param readFrom
     * @return
     */
    public Range read(Path readFrom){
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fileSystem.open(readFrom);
            Range range = new Range();
            range.readFields(fsDataInputStream);
            return range;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
//        RangeReader rangeReader = new RangeReader(configuration);
//        for (Range range : rangeReader.readAll()){
//            System.out.println(range);

    }
}
