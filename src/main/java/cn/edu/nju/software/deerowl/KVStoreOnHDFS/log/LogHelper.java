package cn.edu.nju.software.deerowl.KVStoreOnHDFS.log;

import java.io.*;
import java.util.*;

/**
 * Created by rale on 11/8/17.
 *
 */
public class LogHelper {

    private String logRootPath = "/opt/localdisk/";
    //private String logRootPath = "/Users/soleil/Documents/研究生/大数据/本地测试/";
    private String logPath;
    private ObjectInputStream reader;
    private ObjectOutputStream writer;
    private int maxCount;
    private int currentCount;

    public LogHelper(){
        this.maxCount = maxCount;
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

        System.out.print("write succeed!");

    }

    //readLog List<KV> 要把目录下所有的文件都读出来
    public  List<KvPair> readLog() throws IOException, ClassNotFoundException {

            File file = new File(logRootPath);
            List<KvPair> res = new ArrayList<KvPair>();

            if(file.exists()){
                File[] fileList = file.listFiles();
                for(File logFile : fileList){
                    //判断是否为日志文件
                    if (logFile.getPath().endsWith(".log")){
                        reader = new ObjectInputStream(new FileInputStream(logFile));
                        try{
                            while(true){
                                KvPair kvPair = (KvPair) reader.readObject();
                                res.add(kvPair);
                            }
                        }catch (EOFException e){

                        }finally {
                            reader.close();
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

    //writer持续写入
    private void ensureWriterOpen() throws IOException {
        if(writer == null){
            logPath = generatePath();
            File file = new File(logPath);
            FileOutputStream fileOutputStream = new FileOutputStream(file,true);
            writer = new ObjectOutputStream(fileOutputStream);
        }
    }


    //生成新日志
    private synchronized void refresh() throws IOException {
        writer.close();
        logPath = generatePath();
        writer = new ObjectOutputStream(new FileOutputStream(new File(logPath)));
    }

    //生成唯一日志名
    private String generatePath(){
            return logRootPath + UUID.randomUUID() + ".log";
    }


}
