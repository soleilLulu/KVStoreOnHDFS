package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.Writer;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by rale on 11/9/17.
 * 向HDFS写入文件
 * 文件目录为writeTo
 * HDFS配置为configuration
 */
public class FileWriter implements Writer<Key, Value>{
    private SequenceFile.Writer writer;
    private Configuration configuration;
    private Path writeTo;

    public FileWriter(Configuration configuration, Path path) throws IOException {
        this.configuration = configuration;
        this.writeTo = path;
        ensureOpen();
    }

    /**
     * 写入单个键值对
     * @param key 可比较的序列化键
     * @param value 序列化值
     */
    @Override
    public synchronized void write(Key key, Value value) {
        try {
            ensureOpen();
            writer.append(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 写入多个键值对
     * @param map
     * @throws IOException
     */
    @Override
    public synchronized void write(Map<Key, Value> map) throws IOException {
        ensureOpen();
        Key key = null;
        Value value = null;
        for (Key tempKey : map.keySet()){
            key = new Key(tempKey.getKey());
            value = new Value(map.get(tempKey).getValues());
            writer.append(key, value);
        }
    }

    public long length() throws IOException {
        return writer.getLength();
    }
    /**
     * 关闭输出流
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        writer.sync();
        writer.close();
        writer = null;
    }

    /**
     * 确保输出流不为空
     * @throws IOException
     */
    @Override
    public void ensureOpen() throws IOException {
        if (writer == null){
            writer = getWriter(configuration, writeTo);
        }
    }

    /**
     * 获得文件写出流
     * @param configuration
     * @param path
     * @return
     * @throws IOException
     */
    private static SequenceFile.Writer getWriter(Configuration configuration, Path path) throws IOException {
        return SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(Key.class),
                SequenceFile.Writer.valueClass(Value.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE)
        );
    }

    public static void main(String[] args){
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        Path seqFile = new Path("/kvstore/file.seq");
        try {
            FileWriter fileWriter = new FileWriter(configuration, seqFile);
            fileWriter.write(new Key("234"), new Value(new HashMap<>()));
            fileWriter.write(new Key("abc"), new Value(new HashMap<>()));
            Map<Key, Value> map = new ConcurrentSkipListMap<>();
            Map<String, String> test = new HashMap<>();
            test.put("hello", "world");
            test.put("what","the fuck");
            map.put(new Key("rale"), new Value(test));
            fileWriter.write(map);
            fileWriter.close();
//            SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
//                    SequenceFile.Reader.file(seqFile));
//            Key key = new Key();
//            Value value = new Value();
//            while (reader.next(key, value)) {
//                System.out.println(key);
//                System.out.println(value);
//                System.out.println(key.toString().equals("234"));
//            }
            FileReader fileReader = new FileReader(configuration, seqFile);
            Map<Key, Value> readMap = fileReader.readAll();
            for (Map.Entry<Key, Value> tempSet : readMap.entrySet()){
                System.out.println(tempSet.getKey());
                System.out.println(tempSet.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
