package com.yonyou.zxs.mapreducev2.appcenter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by zengxs on 2017/5/17.
 */
public class AppCenterMapperV2 extends TableMapper<ImmutableBytesWritable, Result> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] TYPE_ID = Bytes.toBytes("type_id");
    public static final byte[] USER_ID = Bytes.toBytes("user_id");
    public static final byte[] YHT_ID = Bytes.toBytes("yht_id");
    public static final byte[] TENANT_ID = Bytes.toBytes("tenant_id");
    public static final byte[] RESOURCE_ID = Bytes.toBytes("res_id");
    public static final byte[] RESOURCE_CODE = Bytes.toBytes("res_code");

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) {
        try {
            if (res == null || res.isEmpty())
                return;
            byte[] tenantId = res.getValue(CF, TENANT_ID);
            byte[] userId = res.getValue(CF, USER_ID);
            String keyout = Bytes.toString(tenantId) + ":" + Bytes.toString(userId);
            String tableName = ((TableSplit) (context.getInputSplit())).getTable().getNameAsString();
            if ("pub_u8_user".equalsIgnoreCase(tableName)) {
                userId = res.getValue(CF, YHT_ID);
                keyout = Bytes.toString(tenantId) + ":" + Bytes.toString(userId);
                context.write(new ImmutableBytesWritable(Bytes.toBytes(keyout)), res);
            } else {
                context.write(new ImmutableBytesWritable(Bytes.toBytes(keyout)), res);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
