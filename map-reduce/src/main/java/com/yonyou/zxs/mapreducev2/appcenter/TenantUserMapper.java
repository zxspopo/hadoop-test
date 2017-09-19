package com.yonyou.zxs.mapreducev2.appcenter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by zengxs on 2017/5/17.
 */
public class TenantUserMapper extends TableMapper<ImmutableBytesWritable, Result> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] USER_ID = Bytes.toBytes("user_id");

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) {
        try {
            if (res == null || res.isEmpty())
                return;

            String tableName = ((TableSplit) (context.getInputSplit())).getTable().getNameAsString();
            if ("tenant_rela_tmp".equalsIgnoreCase(tableName)) {
                context.write(new ImmutableBytesWritable(res.getValue(CF, USER_ID)), res);
            } else {
                context.write(key, res);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
