package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Map;

/**
 * Created by rale on 11/10/17.
 * 读取数据
 */
public interface Reader<K extends WritableComparable, V extends Writable> {

    /**
     * 读取文件中的所有内容
     * @return Map<K, V >
     */
    Map<K, V> readAll() throws IOException;

    /**
     * 关闭读入流
     */
    void close() throws IOException;

    /**
     * 确保读入流是开启的
     */
    void ensureReaderOpen() throws IOException;
}
