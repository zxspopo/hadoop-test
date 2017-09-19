package com.yonyou.zxs.mapreduce.honor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
public class HonorJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("hong_agg_transfer_name", "hongbao_agg_transfer");
        conf.set("hbase.zookeeper.quorum", args[0]);
        conf.set("member_table_name", "hongbao_va_member");
        conf.set("member_table_name", "hongbao_va_member");
        conf.set("hongbao_honor_tag", "hongbao_honor_tag");
        Job job = createSubmittableJob(conf);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static Job createSubmittableJob(Configuration conf) throws IOException {
        String jobName = "honor-transfer-jobs";
        String srcTable = "hongbao_va_transfer";
        String dstTable = "hongbao_agg_transfer";
        Scan sc = new Scan();
        sc.setCaching(10000);
        sc.setCacheBlocks(false);
        Job job = new Job(conf, jobName);
        job.setJarByClass(HonorMapper.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(srcTable, sc, HonorMapper.class, ImmutableBytesWritable.class,
                Result.class, job);
        TableMapReduceUtil.initTableReducerJob(dstTable, null, job);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HonorJob(), args);
        System.exit(exitCode);
    }
}
