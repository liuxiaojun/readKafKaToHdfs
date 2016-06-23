package com.socialmaster.readtopic;

/**
 * Created by liuxiaojun on 2016/6/23.
 */
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import backtype.storm.task.TopologyContext;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFileNameFormat implements FileNameFormat {
    private static final Logger LOG = LoggerFactory.getLogger(MyFileNameFormat.class);

    private String componentId;
    private int taskId;
    private String path = "/data";
    private String prefix = "";
    private String extension = ".txt";
    private String hdfsUrl = "hdfs://ZWBIGDATA01:8020";

    protected transient Configuration hdfsConfig;

    private SimpleDateFormat formatter_day = new SimpleDateFormat("yyyy/MM/dd");
    //private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public MyFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public MyFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public MyFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    public MyFileNameFormat withHdfsUrl(String hdfsUrl){
        this.hdfsUrl = hdfsUrl;
        hdfsConfig.set("fs.default.name", this.hdfsUrl);
        return this;
    }

    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();

        this.hdfsConfig = new Configuration();
        /*
        Map<String, Object> map = (Map<String, Object>)conf.get("hdfs.config");
        if(map != null){
            for(String key : map.keySet()){
                this.hdfsConfig.set(key, String.valueOf(map.get(key)));
            }
        }
        */
    }

    public String getName(long rotation, long timeStamp) {
        java.util.Date curTime = new java.util.Date();
        String filename = formatter.format(curTime);
        LOG.warn("[getName] filename= " + filename);
        return this.prefix + filename + this.extension;
    }

    public String getPath(){
        java.util.Date curTime = new java.util.Date();
        String dir = formatter_day.format(curTime);
        dir = this.path + "/" + dir;

        try
        {
            FileSystem fs = FileSystem.get(this.hdfsConfig);
            Path hd = new Path(dir);
            //if(!fs.exists(hd))
            if(!fs.isDirectory(hd))
            {
                if(!fs.mkdirs(hd))
                {
                    LOG.warn("[Err] mkdir fail- " + dir);
                }
            }
            LOG.warn("[getPath] curTime= " + curTime);
        }
        catch(Exception mExp)
        {
            LOG.warn("[Exp] mkdir hdfs dir: " + mExp);
        }

        return dir;
    }
}