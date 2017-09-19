package com.yonyou.zxs.mapreducev2.appcenter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by zengxs on 2017/5/17.
 */
public class TenantUserRoleMapper extends TableMapper<ImmutableBytesWritable, Result> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] ROLE_ID = Bytes.toBytes("role_id");

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) {
        try {
            if (res == null || res.isEmpty())
                return;

            context.write(new ImmutableBytesWritable(res.getValue(CF, ROLE_ID)), res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
