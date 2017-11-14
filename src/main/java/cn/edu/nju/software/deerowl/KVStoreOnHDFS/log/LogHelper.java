package cn.edu.nju.software.deerowl.KVStoreOnHDFS.log;

import cn.edu.nju.software.deerowl.KVStoreOnHDFS.IO.DBManager;
import cn.edu.nju.software.deerowl.KVStoreOnHDFS.meta.TestConfig;

import java.io.*;
import java.util.*;

/**
 * Created by rale on 11/8/17.
 *
 */
public class LogHelper {

//    private String logRootPath = "/opt/localdisk/";
    //private String logRootPath = "/Users/soleil/Documents/研究生/大数据/本地测试/";
    private String LOG_SUFFIX = TestConfig.LOG_SUFFIX;
    private String logRootPath = TestConfig.LOG_DIR;
    private String logPath;
    private String flushPath;
    private ObjectInputStream reader;
    private ObjectOutputStream writer;
    private int maxCount;
    private int currentCount;
    private DBManager dbManager;

    public LogHelper(int maxCount){
        this.maxCount = maxCount;
    }

    public LogHelper(int maxCount, DBManager dbManager){
        this(maxCount);
        this.dbManager = dbManager;
        mkdirs();
    }
    //writeLog input KV 要可以新建文件
    public synchronized void writeLog(KvPair kvPair) throws IOException {
        currentCount++;
        ensureWriterOpen();
        writer.writeObject(kvPair);
        writer.flush();

        if(currentCount == maxCount){
            refresh();//新建日志文件
            currentCount = 0;
        }

        System.out.println("write succeed! " + currentCount + " times");

    }

    public synchronized void increaseCount(){
        this.currentCount++;
        currentCount %= maxCount;
    }
    public void readLog_write() throws IOException, ClassNotFoundException {
        File file = new File(logRootPath);
        if(file.exists()){
            File[] fileList = file.listFiles();
            for(File logFile : fileList){
                if (logFile.getPath().endsWith(LOG_SUFFIX)){
                    flushPath = logPath = logFile.getAbsolutePath();
                    reader = new ObjectInputStream(new FileInputStream(logFile));
                    try{
                        while(true){
                            KvPair kvPair = (KvPair) reader.readObject();
                            dbManager.write(kvPair.getKey(), kvPair.getValue(), false);
                        }
                    }catch (EOFException e){

                    }finally {
                        reader.close();
                        writer = new AppendableObjectOutputStream(new FileOutputStream(new File(logPath), true));
                    }
                }
            }

        }
    }
    //readLog List<KV> 要把目录下所有的文件都读出来
    public  List<KvPair> readLog() throws IOException, ClassNotFoundException {

            File file = new File(logRootPath);
            List<KvPair> res = new ArrayList<KvPair>();

            if(file.exists()){
                File[] fileList = file.listFiles();
                for(File logFile : fileList){
                    //判断是否为日志文件
                    if (logFile.getPath().endsWith(LOG_SUFFIX)){
                        reader = new ObjectInputStream(new FileInputStream(logFile));
                        try{
                            while(true){
                                KvPair kvPair = (KvPair) reader.readObject();
                                res.add(kvPair);
                            }
                        }catch (EOFException e){

                        }finally {
                            reader.close();
                            flushPath = logPath = logFile.getAbsolutePath();
                            writer = new AppendableObjectOutputStream(new FileOutputStream(new File(logPath), true));
                        }
                    }

                }

            }

            System.out.println("read succeed!");
            return res;

    }

    //删除日志文件
    public synchronized void deleteLog(String removeLogPath){
        if (removeLogPath != null && !removeLogPath.equals("")) {
            File file = new File(removeLogPath);

            if (file.exists() && file.isFile()) {
                file.delete();
            }
        }
    }

    public String getFlushPath(){
        return this.flushPath;
    }

    //writer持续写入
    private void ensureWriterOpen() throws IOException {
        if(writer == null){
            logPath = generatePath();
            File file = new File(logPath);
            if (file.isFile()){
               writer = new AppendableObjectOutputStream(new FileOutputStream(file,true));
            }else{
                writer = new ObjectOutputStream(new FileOutputStream(file));
            }
        }
    }


    //生成新日志
    private synchronized void refresh() throws IOException {
        writer.close();
        flushPath = logPath;
        logPath = generatePath();
        writer = new ObjectOutputStream(new FileOutputStream(new File(logPath)));
    }

    //生成唯一日志名
    private String generatePath(){
            return logRootPath + UUID.randomUUID() + ".log";
    }

    private void mkdirs(){
        File file = new File(logRootPath);
        if (!file.exists()){
            file.mkdirs();
        }
    }

}
