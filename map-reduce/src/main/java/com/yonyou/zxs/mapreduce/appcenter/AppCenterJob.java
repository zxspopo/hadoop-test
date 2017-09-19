package com.yonyou.zxs.mapreduce.appcenter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zengxs on 2017/7/1.
 */
public class AppCenterJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("hbase.zookeeper.quorum", args[0]);
        conf.set("tenant.table.name", "market_pub_tenant");
        conf.set("tenant.res.table.name", "tenant_user_res");
        conf.set("tenant.user.table.name", "market_pub_tenant_user");
//        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 6000000);
//        conf.setInt("mapreduce.task.timeout", 1800000);
//        conf.set(
//                "mapred.child.java.opts",
//                "-XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:GCTimeRatio=10 -XX:YoungGenerationSizeIncrement=20 -XX:TenuredGenerationSizeIncrement=20 -XX:AdaptiveSizeDecrementScaleFactor=2 -Xmx2000m");
        Job job = createSubmittableJob(conf);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static Job createSubmittableJob(Configuration conf) throws IOException {
        String jobName = "appcenter-tenant-resource-job";
        String srcTable = "tenant_user_rela";
        String dstTable = "appcenter_tenant_user_res";
        Scan sc = new Scan();
        sc.setCaching(10000);
        sc.setCacheBlocks(false);
        Job job = new Job(conf, jobName);
        job.setJarByClass(AppCenterMapper.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(srcTable, sc, AppCenterMapper.class, ImmutableBytesWritable.class,
                Result.class, job);
        TableMapReduceUtil.initTableReducerJob(dstTable, null, job);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AppCenterJob(), args);
        System.exit(exitCode);
    }
}
