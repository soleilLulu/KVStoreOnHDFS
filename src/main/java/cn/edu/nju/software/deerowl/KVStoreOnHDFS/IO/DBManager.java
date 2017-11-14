package cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.Cache;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.FileReader;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.hdfs.FileWriter;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.IndexWriter;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.Range;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.index.RangeManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Key;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.model.Value;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.KvPair;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.log.LogHelper;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by rale on 11/10/17.
 * 数据库操作
 */
public class DBManager {

    private String DATA_DIR = TestConfig.DATA_DIR;
    private String RANGE_DIR = TestConfig.RANGE_DIR;
    private String INDEX_DIR = TestConfig.DATA_DIR;
    private String LOG_DIR = TestConfig.LOG_DIR;

    private String DATA_SUFFIX = TestConfig.DATA_SUFFIX;
    private String INDEX_SUFFIX = TestConfig.INDEX_SUFFIX;

    private Cache<Key, Value> cache;
    private Configuration configuration;
    private RangeManager rangeManager;

    private LogHelper logHelper;

    private FileSystem fileSystem;
    public DBManager(Configuration configuration) throws IOException, ClassNotFoundException {
        this.configuration = configuration;
        this.cache = new Cache<>(TestConfig.MAX_CACHE);
        rangeManager = new RangeManager(configuration);

        //从日志中读出尚未存储的数据
        logHelper = new LogHelper(TestConfig.MAX_LOG,this);
        logHelper.readLog_write();

        fileSystem = FileSystem.get(configuration);
        mkdirs();
    }

    /**
     * 将数据输出到输出流
     * @param key
     * @param value
     */
    public void write(String key, Map<String, String> value, boolean writeLog) throws IOException {
        Key key1 = new Key(key);
        Value value1 = new Value(value);
        cache.add(key1, value1);
        if (writeLog){
            logHelper.writeLog(new KvPair(key, value));
        }else {
            logHelper.increaseCount();
        }
        if (cache.isFull()){
            String flushPath = logHelper.getFlushPath();
            ConcurrentSkipListMap<Key, Value> copyCache = cache.getAll();
            cache.refresh();
            new Runnable() {
                @Override
                public void run() {
                    String fileName = generateFileName();
                    Path dataPath = new Path(DATA_DIR + fileName + DATA_SUFFIX);
                    Path indexPath = new Path(INDEX_DIR + fileName + INDEX_SUFFIX);
                    try {
                        FileWriter fileWriter = new FileWriter(configuration, dataPath);
                        IndexWriter indexWriter = new IndexWriter(configuration, indexPath);
                        for (Map.Entry<Key, Value> entry : copyCache.entrySet()){
                            fileWriter.write(entry.getKey(), entry.getValue());
                            indexWriter.write(entry.getKey(), new LongWritable(fileWriter.length()));
                        }
                        rangeManager.write(new Range(copyCache.firstKey(), copyCache.lastKey(), new Text(indexPath.toString())));
                        //@Todo 向所有主机发送一条通信 说明二级索引更新了
                        logHelper.deleteLog(flushPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.run();
        }

    }


    public Map<String, String> read(String key){
        Key key1 = new Key(key);
        Value result  = null;
        if ((result = cache.find(key1)) != null){
            return result.getMap();
        }
        //从邻居获得
        //检查二级索引
        //检查一级索引
        //从文件中获取
        return new HashMap<>();
    }

    private String generateFileName(){
        return UUID.randomUUID().toString();
    }

    private void mkdirs() throws IOException {
        Path data = new Path(DATA_DIR);
        if (!fileSystem.exists(data)){
            fileSystem.mkdirs(data);
        }
        Path range = new Path(RANGE_DIR);
        if (!fileSystem.exists(range)){
            fileSystem.mkdirs(range);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        DBManager dbManager = new DBManager(configuration);
        System.out.println(dbManager.cache.size());
        dbManager.write("name", new HashMap<>(), true);

    }


}
