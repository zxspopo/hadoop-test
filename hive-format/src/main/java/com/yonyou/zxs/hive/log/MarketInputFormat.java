package com.yonyou.zxs.hive.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

/**
 * Created by zengxs on 2017/8/14.
 */
public class MarketInputFormat extends TextInputFormat implements JobConfigurable {

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        return new MarketReader(job, (FileSplit) genericSplit);
    }
}
