package com.yonyou.zxs.mapreducev2.appcenter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zengxs on 2017/5/17.
 */
public class TenantUserReduce extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] USER_EMAIL = Bytes.toBytes("user_email");
    public static final byte[] USER_NAME = Bytes.toBytes("user_name");
    public static final byte[] VALIDATE_MOBILE = Bytes.toBytes("validate_mobile");
    public static final byte[] SYSTEM_ID = Bytes.toBytes("system_id");
    public static final byte[] VERIFIED = Bytes.toBytes("verified");
    public static final byte[] VALIDATE_EMAIL = Bytes.toBytes("validate_email");
    public static final byte[] TYPE_ID = Bytes.toBytes("type_id");
    public static final byte[] REGISTER_DATE = Bytes.toBytes("register_date");
    public static final byte[] NEED_MERGE = Bytes.toBytes("need_merge");
    public static final byte[] USER_AVATOR = Bytes.toBytes("user_avator");
    public static final byte[] USER_CODE = Bytes.toBytes("user_code");
    public static final byte[] USER_ID = Bytes.toBytes("user_id");
    public static final byte[] USER_MOBILE = Bytes.toBytes("user_mobile");
    public static final byte[] USER_AVATOR_NEW = Bytes.toBytes("user_avator_new");
    public static final byte[] TENANT_FULLNAME = Bytes.toBytes("tenant_fullname");
    public static final byte[] TENANT_CODE = Bytes.toBytes("tenant_code");
    public static final byte[] TENANT_ID = Bytes.toBytes("tenant_id");
    public static final byte[] TENANT_NAME = Bytes.toBytes("tenant_name");


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException,
            InterruptedException {
        List<Result> tenantResList = new ArrayList<>();
        Result userRes = null;
        for (Result each_val : values) {
            if (!each_val.isEmpty()) {
                boolean isTenantTable = each_val.containsColumn(CF, TENANT_FULLNAME);
                if (isTenantTable) {
                    tenantResList.add(each_val);
                } else {
                    userRes = ((each_val == null || each_val.isEmpty()) ? userRes : each_val);
                }
            }
        }
        if (userRes != null && !tenantResList.isEmpty()) {
            for (Result tenantRes : tenantResList) {
                String tenantId = Bytes.toString(tenantRes.getValue(CF, TENANT_ID));
                String userId = Bytes.toString(userRes.getRow());
                Put put = new Put(Bytes.toBytes(tenantId + ":" + userId));
                put.addColumn(CF, TENANT_NAME, tenantRes.getValue(CF, TENANT_NAME));
                put.addColumn(CF, TENANT_CODE, tenantRes.getValue(CF, TENANT_CODE));
                put.addColumn(CF, TENANT_FULLNAME, tenantRes.getValue(CF, TENANT_FULLNAME));
                put.addColumn(CF, USER_EMAIL, userRes.getValue(CF, USER_EMAIL));
                put.addColumn(CF, USER_NAME, userRes.getValue(CF, USER_NAME));
                put.addColumn(CF, VALIDATE_MOBILE, userRes.getValue(CF, VALIDATE_MOBILE));
                put.addColumn(CF, SYSTEM_ID, userRes.getValue(CF, SYSTEM_ID));
                put.addColumn(CF, VERIFIED, userRes.getValue(CF, VERIFIED));
                put.addColumn(CF, VALIDATE_EMAIL, userRes.getValue(CF, VALIDATE_EMAIL));
                put.addColumn(CF, REGISTER_DATE, userRes.getValue(CF, REGISTER_DATE));
                put.addColumn(CF, NEED_MERGE, userRes.getValue(CF, NEED_MERGE));
                put.addColumn(CF, USER_AVATOR, userRes.getValue(CF, USER_AVATOR));
                put.addColumn(CF, USER_CODE, userRes.getValue(CF, USER_CODE));
                put.addColumn(CF, USER_ID, userRes.getRow());
                put.addColumn(CF, USER_MOBILE, userRes.getValue(CF, USER_MOBILE));
                put.addColumn(CF, USER_AVATOR_NEW, userRes.getValue(CF, USER_AVATOR_NEW));
                put.addColumn(CF, TYPE_ID, tenantRes.getValue(CF, TYPE_ID));
                put.addColumn(CF, TENANT_ID, tenantRes.getValue(CF, TENANT_ID));
                context.write(key, put);
            }
        }
    }
}
