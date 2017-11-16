package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.Reader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rale on 11/10/17.
 * 从HDFS读取文件
 */
public class FileReader implements Reader<Key, Value> {
    private Path readFrom;
    private Configuration configuration;
    private SequenceFile.Reader reader;

    public FileReader(Configuration configuration, Path readFrom) throws IOException {
        this.readFrom = readFrom;
        this.configuration = configuration;
        reader = getReader(configuration, readFrom);
    }

    public Value findFrom(long pos, Key key) throws IOException {
        ensureReaderOpen();
        reader.seek(pos);
        Key tempKey = new Key();
        Value tempValue = new Value();
        while(reader.next(tempKey, tempValue)){
            if (tempKey.compareTo(key) == 0){
                return tempValue;
            }
        }
        return null;
    }

    @Override
    public Map<Key, Value> readAll() throws IOException {
        ensureReaderOpen();
        Map<Key, Value> map = new HashMap<>();
        Key key = new Key();
        Value value = new Value();
        while (reader.next(key, value)){
            map.put(key, value);
            key = new Key();
            value = new Value();
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
        if (reader == null){
            reader = getReader(configuration, readFrom);
        }
    }

    private static SequenceFile.Reader getReader(Configuration configuration , Path readFrom) throws IOException {
        return new SequenceFile.Reader(configuration,
                SequenceFile.Reader.file(readFrom));
    }
}
