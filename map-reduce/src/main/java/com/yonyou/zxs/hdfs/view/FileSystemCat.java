package com.yonyou.zxs.hdfs.view;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 通过FileSystem的api读取hdfs上的文件内容 Created by zengxs on 2016/12/12.
 */
public class FileSystemCat {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
