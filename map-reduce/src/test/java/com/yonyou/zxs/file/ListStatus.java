package com.yonyou.zxs.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Created by zengxs on 2016/12/20.
 */
public class ListStatus {

    @Test
    public void listPath() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://172.20.14.61:9000/data"), conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path("hdfs://172.20.14.61:9000/data"));
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path path : paths) {
            System.out.println(path);
        }
    }

}
