package com.yonyou.zxs.mapreducev2.appcenter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zengxs on 2017/5/17.
 */
public class TenantRelaReduce extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] TENANT_FULLNAME = Bytes.toBytes("tenant_fullname");
    public static final byte[] TENANT_CODE = Bytes.toBytes("tenant_code");
    public static final byte[] TENANT_ID = Bytes.toBytes("tenant_id");
    public static final byte[] TENANT_NAME = Bytes.toBytes("tenant_name");


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException,
            InterruptedException {
        Result tenantRes = null;
        List<Result> relaResList = new ArrayList<>();
        for (Result each_val : values) {
            if (!each_val.isEmpty()) {
                boolean isRelaTable = each_val.containsColumn(CF, TENANT_ID);
                if (!isRelaTable) {
                    tenantRes = each_val;
                } else {
                    relaResList.add(each_val);
                }
            }
        }
        if (!relaResList.isEmpty() && tenantRes != null) {
            for (Result relaRes : relaResList) {
                Put put = new Put(relaRes.getRow());
                put.addColumn(CF, TENANT_NAME, tenantRes.getValue(CF, TENANT_NAME));
                put.addColumn(CF, TENANT_CODE, tenantRes.getValue(CF, TENANT_CODE));
                put.addColumn(CF, TENANT_FULLNAME, tenantRes.getValue(CF, TENANT_FULLNAME));
                for (Cell kv : relaRes.listCells()) {
                    put.addColumn(CF, kv.getQualifier(), kv.getValue());
                }
                context.write(key, put);
            }
        }

    }
}
