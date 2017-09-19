package com.yonyou.zxs.hdfs.write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * Created by zengxs on 2016/12/20.
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws IOException {
        // copyWithStartParam(args);
        cpTemperature();
    }

    /**
     * 集群连接
     * 
     * @throws IOException
     */
    private static void cpTemperature() throws IOException {
        String localpath = "D:\\opensource\\hadoop\\map-reduce\\src\\main\\resources\\maxtemperature\\sample.txt";

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hdfs");
        conf.set("dfs.nameservices", "hdfs");
        conf.set("dfs.ha.namenodes.hdfs", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.hdfs.nn1", "192.168.32.11:50071");
        conf.set("dfs.namenode.rpc-address.hdfs.nn2", "192.168.32.10:50071");
        conf.set("dfs.client.failover.proxy.provider.hdfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        cp(localpath, "/weather/data2", conf);

    }

    private static void copyWithStartParam(String[] args) throws IOException {

        Configuration conf = new Configuration();
        String localPath = args[0];
        String dst = args[1];
        cp(localPath, dst, conf);
    }

    private static void cp(String localPath, String dst, Configuration conf) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(localPath));

        FileSystem fileSystem = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fileSystem.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

}
