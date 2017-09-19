package com.yonyou.zxs.mapreduce.temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper 输入键，输入值，输出值，输出值类型 Created by zengxs on 2016/12/8.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(0, 4);
        int airTemperature;
        try {
            if (line.charAt(15) == '+') {
                airTemperature = Integer.parseInt(line.substring(16, 18).trim());
            } else {
                airTemperature = Integer.parseInt(line.substring(15, 18).trim());
            }
        } catch (Exception e) {
            airTemperature = MISSING;
        }
        // String quality = line.substring(92, 93);
        if (airTemperature != MISSING) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
