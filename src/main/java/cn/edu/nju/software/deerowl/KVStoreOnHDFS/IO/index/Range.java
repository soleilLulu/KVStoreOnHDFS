package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rale on 11/9/17.
 * 二级索引
 * 包括一万条数据的最小键值和最大键值以及存储这个文件的位置
 */
public class Range implements Writable{
    private Key startIndex;
    private Key endIndex;
    private Text path;

    public Range(String startIndex, String endIndex, String path){
        this.startIndex = new Key(startIndex);
        this.endIndex = new Key(endIndex);
        this.path = new Text(path);
    }

    public Range(Key startIndex, Key endIndex, Text path){
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.path = path;
    }

    public Range(){
        startIndex = new Key();
        endIndex = new Key();
        path = new Text();
    }

    /**
     * 某个键值是否位于当前的范围集内
     * @param key
     * @return
     */
    public boolean contains(Key key){
        if (this.getStartIndex().compareTo(key) <= 0
                && this.getEndIndex().compareTo(key) >= 0){
            return true;
        }
        return false;
    }

    /**
     * 某些键值是否位于当前的范围集内，任何一个位于即位于
     * @param keys
     * @return
     */
    public boolean contains(Key ...keys){
        for (Key key : keys){
            if (contains(key)) return true;
        }
        return false;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        startIndex.write(dataOutput);
        endIndex.write(dataOutput);
        path.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startIndex.readFields(dataInput);
        endIndex.readFields(dataInput);
        path.readFields(dataInput);
    }

    public Key getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(Key startIndex) {
        this.startIndex = startIndex;
    }

    public Key getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(Key endIndex) {
        this.endIndex = endIndex;
    }

    public Text getPath() {
        return path;
    }

    public void setPath(Text path) {
        this.path = path;
    }

    @Override
    public String toString(){
        return startIndex.toString() + endIndex.toString() + path.toString();
    }

}
