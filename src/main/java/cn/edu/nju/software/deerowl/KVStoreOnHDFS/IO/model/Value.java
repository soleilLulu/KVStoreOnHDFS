package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model;

import com.google.gson.Gson;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rale on 11/9/17.
 * 值属性全部封装为byte流并实现序列化接口
 */
public class Value implements Writable {
    private BytesWritable values;

    public Value(){
        values = new BytesWritable();
    }

    public Value(BytesWritable value){
        this.setValues(value);
    }

    public Value(Map<String, String> map){
        this.setValues(map);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        values.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        values.readFields(dataInput);
    }

    @Override
    public String toString(){
        return getString();
    }
    public BytesWritable getValues() {
        return values;
    }

    public void setValues(BytesWritable values) {
        this.values = values;
    }
    public void setValues(byte[] bytes){
        this.values = new BytesWritable(bytes);
    }
    public void setValues(Map<String, String> values){
        this.values = new BytesWritable(new Gson().toJson(values).getBytes());
    }

    /**
     * 获取值的字符串表达
     * @return
     */
    public String getString(){
        return new String(values.getBytes(), 0, values.getLength());
    }

    /**
     * 获取值的map表达
     * @return
     */
    public Map<String, String> getMap(){
        if(values == null || values.getLength()==0) return new HashMap<>();
        return new Gson().fromJson(getString(), HashMap.class);
    }

}
