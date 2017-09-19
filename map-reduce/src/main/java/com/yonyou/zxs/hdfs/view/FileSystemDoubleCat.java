package com.yonyou.zxs.hdfs.view;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * FSDataInputStream对象
 * <P>
 * FileSystem open()返回的是FSDataInputStream对象，而不是标准的java.io对象。他是继承了java.io.DataInputStream的一个特殊类。
 * </P>
 * <p>
 * seek()可以定位到文件长度以内的任何一个位置。和skip()不同，skip只能根据当前位置定位到另一个新位置。
 * </p>
 * <p>
 * 下面的示例我们将输出两遍文件内容
 * </p>
 * <p>
 *     seek()是一个高开销的动作,需要谨慎使用。建议使用流数据来构建应用的访问模式（如mapreduce），而不是seek()
 * </p>
 *
 * Created by zengxs on 2016/12/13.
 */
public class FileSystemDoubleCat {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);// go back to the start of the file
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
