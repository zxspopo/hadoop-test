package com.yonyou.zxs.mapreducev2.appcenter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by zengxs on 2017/5/17.
 */
public class TenantRelaMapper extends TableMapper<ImmutableBytesWritable, Result> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] USER_ID = Bytes.toBytes("user_id");
    public static final byte[] TENANT_FULLNAME = Bytes.toBytes("tenant_fullname");
    public static final byte[] TENANT_CODE = Bytes.toBytes("tenant_code");
    public static final byte[] TENANT_ID = Bytes.toBytes("tenant_id");
    public static final byte[] TENANT_NAME = Bytes.toBytes("tenant_name");

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) {
        try {
            if (res == null || res.isEmpty())
                return;
            String tableName = ((TableSplit) (context.getInputSplit())).getTable().getNameAsString();

            if ("tenant_user_rela".equalsIgnoreCase(tableName)) {
                byte[] tenantId = res.getValue(CF, TENANT_ID);
                context.write(new ImmutableBytesWritable(tenantId), res);
            } else if ("market_pub_tenant".equalsIgnoreCase(tableName)) {
                context.write(key, res);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
