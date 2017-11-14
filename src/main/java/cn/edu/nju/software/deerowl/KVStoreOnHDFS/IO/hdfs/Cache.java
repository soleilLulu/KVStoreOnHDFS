package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Value;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.KvPair;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by rale on 11/10/17.
 * 内存缓存类
 */
public class Cache<K extends WritableComparable, V extends Writable> {
    private ConcurrentSkipListMap<K, V> cache;
    private int maxCache = TestConfig.MAX_CACHE;
    public Cache(int maxCache){
        cache = new ConcurrentSkipListMap<>();
        this.maxCache = maxCache;
    }
    

    /**
     * 向缓存中写入数据
     * @param key
     * @param value
     */
    public void add(K key, V value){
        cache.put(key, value);
    }

    /**
     * 获得数据列表
     * @return
     */
    public ConcurrentSkipListMap<K, V> getAll(){
        return cache;
    }

    /**
     * 缓存是否满了
     * @return
     */
    public boolean isFull(){
        return maxCache == size();
    }

    /**
     * 缓存大小
     * @return
     */
    public int size(){
        return cache == null ?  0 : cache.size();
    }

    /**
     * 根据key找到value
     * @param key
     * @return
     */
    public V find(K key){
        return cache.get(key);
    }

    /**
     * 清空缓存
     */
    public void clear(){
        cache = null;
    }

    public void refresh(){
        clear();
        cache = new ConcurrentSkipListMap<>();
    }
}
