package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rale on 11/9/17.
 * 封装Key类型，将所有的键统一存储为Bytes流输出到文件中
 */
public class Key implements WritableComparable<Key> {
    private BytesWritable key;

    public Key(){
        key = new BytesWritable();
    }

    public Key(BytesWritable key){
        this.key = key;
    }

    public Key(String key){
        this.setKey(key);
    }

    public Key(byte[] bytes){
        this.setKey(bytes);
    }

    @Override
    public int compareTo(Key o) {
        return key.compareTo(o.getKey());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
    }


    public BytesWritable getKey() {
        return key;
    }

    public void setKey(BytesWritable key) {
        this.key = key;
    }

    public void setKey(String key){
        this.setKey(key.getBytes());
    }

    public void setKey(byte[] key){
        this.key = new BytesWritable(key);
    }

    @Override
    public String toString(){
        return new String(this.key.getBytes(), 0, this.key.getLength());
    }
}
