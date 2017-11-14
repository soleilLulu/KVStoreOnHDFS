package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.Writer;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Map;


/**
 * Created by rale on 11/9/17.
 * 写出至一级索引类
 */
public class IndexWriter implements Writer<Key, LongWritable>{

    private SequenceFile.Writer writer;
    private Path currentWritePath;
    private Configuration configuration;

    public IndexWriter(Configuration configuration, Path path) throws IOException {
        this.configuration = configuration;
        this.currentWritePath = path;
        this.writer = getWriter(configuration, path);
    }

    public IndexWriter(Configuration configuration, String path) throws IOException {
        this(configuration, new Path(path));
    }


    @Override
    public void write(Key key, LongWritable value) throws IOException {
        ensureOpen();
        writer.append(key, value);
    }

    @Override
    public void write(Map<Key, LongWritable> map) throws IOException {
        ensureOpen();
        for (Map.Entry<Key, LongWritable> entry : map.entrySet()){
            write(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 确保文件输出流存在
     * @throws IOException
     */
    public void ensureOpen() throws IOException {
        if (writer == null){
            writer = getWriter(configuration,currentWritePath);
        }
    }

    /**
     * 关闭输出流
     * @throws IOException
     */
    public void close() throws IOException {
        this.writer.sync();
        this.writer.close();
        this.writer = null;
    }

    private static SequenceFile.Writer getWriter(Configuration configuration, Path path) throws IOException {
        return SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(Key.class),
                SequenceFile.Writer.valueClass(LongWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE)
        );
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        IndexWriter indexWriter = new IndexWriter(configuration, new Path("/data/test"));
        indexWriter.write(new Key("a"), new LongWritable(100));
        indexWriter.write(new Key("e"), new LongWritable(200));
        indexWriter.close();
        IndexReader indexReader = new IndexReader(configuration, new Path("/data/test"));
        for (Map.Entry<Key, LongWritable> entry : indexReader.readAll().entrySet()){
            System.out.println(entry.getKey());
        }
    }
}
