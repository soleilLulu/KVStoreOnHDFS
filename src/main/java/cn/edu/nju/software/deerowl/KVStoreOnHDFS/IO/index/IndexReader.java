package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.Reader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by rale on 11/9/17.
 * 读入一级索引类
 */
public class IndexReader implements Reader<Key, LongWritable>{

    private SequenceFile.Reader reader;
    private Configuration configuration;
    private Path readFrom;

    public IndexReader(Configuration configuration, Path path) throws IOException {
        this.configuration = configuration;
        this.readFrom = path;
        ensureReaderOpen();
    }


    /**
     * 根据索引找到距离键最近的位置
     * @param key
     * @return
     * @throws IOException
     */
    public long getPosition(Key key) throws IOException {
        TreeMap<Key, LongWritable> sortedMap = new TreeMap<>(readAll());
        long position = 0;
        for (Map.Entry<Key, LongWritable> entry : sortedMap.entrySet()){
            if (entry.getKey().compareTo(key) == 0){
                return entry.getValue().get();
            } else if (entry.getKey().compareTo(key) < 0){
                position = entry.getValue().get();
            }else {
                break;
            }
        }
        return position;
    }

    @Override
    public Map<Key, LongWritable> readAll() throws IOException {
        ensureReaderOpen();
        Map<Key, LongWritable> map = new HashMap<>();
        Key key = new Key();
        LongWritable value = new LongWritable();
        while (reader.next(key, value)){
            map.put(key, value);
            key = new Key();
            value = new LongWritable();
        }
        return map;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        reader = null;
    }

    @Override
    public void ensureReaderOpen() throws IOException {
        if (reader==null){
            reader = getReader(configuration, readFrom);
        }
    }

    private static SequenceFile.Reader getReader(Configuration configuration, Path path) throws IOException {
        return new SequenceFile.Reader(
                configuration,
                SequenceFile.Reader.file(path));
    }

}
