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
public class TenantUserRoleReduce extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] USER_ID = Bytes.toBytes("user_id");
    public static final byte[] TENANT_ID = Bytes.toBytes("tenant_id");
    public static final byte[] ROLE_ID = Bytes.toBytes("role_id");
    public static final byte[] ROLE_CODE = Bytes.toBytes("role_code");
    public static final byte[] ROLE_NAME = Bytes.toBytes("role_name");


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException,
            InterruptedException {
        List<Result> userRoleResList = new ArrayList<>();
        Result tenantRoleRes = null;
        for (Result each_val : values) {
            if (!each_val.isEmpty()) {
                boolean isUserRoleTable = each_val.containsColumn(CF, USER_ID);
                if (isUserRoleTable) {
                    userRoleResList.add(each_val);
                } else {
                    tenantRoleRes = each_val;
                }
            }
        }

        // 将 role 1--n user 转换为 user-role
        if (tenantRoleRes != null && !userRoleResList.isEmpty()) {
            for (Result userRoleRes : userRoleResList) {
                String tenantId = Bytes.toString(tenantRoleRes.getValue(CF, TENANT_ID));
                String userId = Bytes.toString(userRoleRes.getValue(CF, USER_ID));
                String roleId = Bytes.toString(tenantRoleRes.getValue(CF, ROLE_ID));
                Put put = new Put(Bytes.toBytes(tenantId + ":" + userId + ":" + roleId));
                put.addColumn(CF, TENANT_ID, Bytes.toBytes(tenantId));
                put.addColumn(CF, USER_ID, Bytes.toBytes(userId));
                put.addColumn(CF, ROLE_ID, Bytes.toBytes(roleId));
                put.addColumn(CF, ROLE_CODE, tenantRoleRes.getValue(CF, ROLE_CODE));
                put.addColumn(CF, ROLE_NAME, tenantRoleRes.getValue(CF, ROLE_NAME));
                context.write(key, put);
            }
        }
    }
}
