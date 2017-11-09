package cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta;

import cn.helium.kvstore.common.KvStoreConfig;

/**
 * Created by rale on 11/8/17.
 */
public class Config {

    //日志目录
    public static final String LOG_DIRECTORY = "/opt/localdisk";

    //hdfs位置
    public static final String HDFS_URL =  KvStoreConfig.getHdfsUrl();

}
