package cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcServer;

/**
 * Created by rale on 11/8/17.
 */
public class TestConfig {


    public static final String HDFS_URL = KvStoreConfig.getHdfsUrl();
//    public static final String HDFS_URL = "127.0.0.1:8020";
    public static final int INDEX_INTERVAL = 10;
    public static final int MAX_CACHE = 50;
    public static final int MAX_LOG = 50;


    public static final String LOG_DIR = "/Users/rale/Desktop/log/";
    public static final String RANGE_DIR = "/range/";
    public static final String DATA_DIR = "/data/";

    public static final String LOG_SUFFIX = ".log";
    public static final String INDEX_SUFFIX = ".idx";
    public static final String DATA_SUFFIX = ".seq";

//    public static final int SERVER_ID = RpcServer.getRpcServerId();
}
