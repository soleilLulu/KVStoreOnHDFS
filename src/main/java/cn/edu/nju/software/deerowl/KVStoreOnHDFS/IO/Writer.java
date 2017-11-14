package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Map;

/**
 * Created by rale on 11/10/17.
 * 写出数据
 */
public interface Writer<K extends WritableComparable, V extends Writable> {
    /**
     * 写入单条记录
     * @param key 可比较的序列化键
     * @param value 序列化值
     */
    void write(K key, V value) throws IOException;

    /**
     * 一次性写入多条记录
     * @param map
     */
    void write(Map<K, V> map) throws IOException;

    /**
     * 关闭流
     */
    void close() throws IOException;

    /**
     * 确保当前流为开启状态
     */
    void ensureOpen() throws IOException;

}
