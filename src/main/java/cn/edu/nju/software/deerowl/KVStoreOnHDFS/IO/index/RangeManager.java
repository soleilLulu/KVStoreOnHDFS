package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rale on 11/9/17.
 * 二级索引管理类
 * 包括读入写出二级索引
 * 目前二级索引的数据量不是很大，一万条数据产生一条二级索引，因此准备全部读入内存
 */
public class RangeManager {

    private String RANGE_DIR = TestConfig.RANGE_DIR;
    private RangeReader rangeReader;
    private RangeWriter rangeWriter;
    private Configuration configuration;
    private List<Range> ranges;

    public RangeManager(Configuration configuration){
        this.configuration = configuration;
        ensureOpen();
        refresh();
    }

    public RangeManager(Configuration configuration, String dir){
        this(configuration);
        this.RANGE_DIR = dir;
    }
    /**
     * 更新二级索引
     */
    public void refresh(){
        clear();
        ensureOpen();
        ranges = rangeReader.readAll();
    }

    /**
     * 当缓存中的二级索引数小于实际的二级索引数的时候，需要更新二级索引
     * @return
     */
    public boolean needRefresh(){
        return ranges.size() < rangeReader.numOfRanges();
    }


    /**
     * 将二级缓存写至本地
     * @param range
     */
    public void write(Range range){
        ensureOpen();
        rangeWriter.write(range);
    }

    /**
     * @Todo 还需要注意 需要向远程提出添加请求 在这里的话应当添加一个参数 表明是否向远处发出添加消息
     * 记录二级缓存信息，如果缓存信息包含
     * @param range
     * @param writeToLocal
     */
    public void write(Range range, boolean writeToLocal){
        ranges.add(range);
        if (writeToLocal){
            write(range);
        }
    }
    /**
     * 找到包含某个键的二级索引
     * @param keys
     * @return
     */
    public List<Range> findRangeContainsKey(Key ...keys){
        ensureOpen();
        if (needRefresh()) refresh();
        List<Range> result = new ArrayList<>();
        for(Range range : ranges){
            if (range.contains(keys)) result.add(range);
        }
        return result;
    }



    private void ensureOpen(){
        if (rangeReader==null) try {
            rangeReader = new RangeReader(configuration, RANGE_DIR);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rangeWriter==null) try {
            rangeWriter = new RangeWriter(configuration, RANGE_DIR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void clear(){
        ranges = null;
    }

}
