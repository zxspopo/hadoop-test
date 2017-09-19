package com.yonyou.zxs.hdfs.view;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * 通过URL读取的方式读取hdfs上的内容
 * <p>
 * runCmd
 * </p>
 * <p>
 * hadoop jar map-reduce-1.0-SNAPSHOT.jar com.yonyou.zxs.hdfs.URLCat
 * hdfs://192.168.56.101:9000/data/weather/in
 * </p>
 * 
 * Created by zengxs on 2016/12/12.
 */
public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

}
