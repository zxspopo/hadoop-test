package com.yonyou.zxs.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by zengxs on 2016/12/20.
 */
public class ShowFileStatus {

    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create("hdfs://172.20.14.61:9000/data"), conf);
        OutputStream out = fs.create(new Path("hdfs://172.20.14.61:9000/data/log/iuap.log"));
        out.write("content".getBytes("utf-8"));
        out.close();
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(new Path("hdfs://172.20.14.61:9000/data/log"), true);
        if (fs != null) {
            fs.close();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testFileNotFount() throws IOException {
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("hdfs://172.20.14.61:9000/data/log/iuap.log");
        FileStatus fileStatus = fs.getFileStatus(file);
        Assert.assertEquals(fileStatus.getPath().toUri().getPath(), "/data/log/iuap.log");
        Assert.assertFalse(fileStatus.isDirectory());
        Assert.assertEquals(fileStatus.getLen(), 7L);
        Assert.assertTrue(fileStatus.getModificationTime() < System.currentTimeMillis());
        Assert.assertEquals(fileStatus.getReplication(), 3);
        Assert.assertEquals(fileStatus.getBlockSize(), 2 * 64 * 1024 * 1024L);
        Assert.assertEquals(fileStatus.getOwner(), "zengxs");
        Assert.assertEquals(fileStatus.getGroup(), "supergroup");
        Assert.assertEquals(fileStatus.getPermission().toString(), "rw-r--r--");
    }

    @Test
    public void fileStatusForDir() throws IOException {
        Path path = new Path("hdfs://172.20.14.61:9000/data/log");
        FileStatus fileStatus = fs.getFileStatus(path);
        Assert.assertEquals(fileStatus.getPath().toUri().getPath(), "/data/log");
        Assert.assertTrue(fileStatus.isDirectory());
        Assert.assertEquals(fileStatus.getLen(), 0);
        Assert.assertTrue(fileStatus.getModificationTime() < System.currentTimeMillis());
        Assert.assertEquals(fileStatus.getReplication(), 0);
        Assert.assertEquals(fileStatus.getBlockSize(), 0);
        Assert.assertEquals(fileStatus.getOwner(), "zengxs");
        Assert.assertEquals(fileStatus.getGroup(), "supergroup");
        Assert.assertEquals(fileStatus.getPermission().toString(), "rwxr-xr-x");
    }
}
