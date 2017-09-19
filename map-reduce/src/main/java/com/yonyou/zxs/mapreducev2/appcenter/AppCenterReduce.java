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
public class AppCenterReduce extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] TYPE_ID = Bytes.toBytes("type_id");
    public static final byte[] RESOURCE_ID = Bytes.toBytes("res_id");
    public static final byte[] RESOURCE_CODE = Bytes.toBytes("res_code");
    public static final byte[] ROLE_ID = Bytes.toBytes("role_id");
    public static final byte[] ROLE_CODE = Bytes.toBytes("role_code");
    public static final byte[] ROLE_NAME = Bytes.toBytes("role_name");
    public static final byte[] U8_USERNAME = Bytes.toBytes("u8_username");
    public static final byte[] U8_USERCODE = Bytes.toBytes("u8_usercode");
    public static final byte[] DOMAIN_ID = Bytes.toBytes("domain_id");

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException,
            InterruptedException {
        List<Result> resList = new ArrayList();
        List<Result> roleList = new ArrayList();
        List<Result> u8List = new ArrayList<>();
        List<Result> domainList = new ArrayList<>();
        Result tenantUserRelaResult = null;
        for (Result each_val : values) {
            if (!each_val.isEmpty()) {

                // tenant-user-rela
                boolean isRela = each_val.containsColumn(CF, TYPE_ID);
                if (isRela) {
                    tenantUserRelaResult = each_val;
                } else {

                    // tenant_role_tmp
                    boolean isRole = each_val.containsColumn(CF, ROLE_ID);
                    if (isRole) {
                        roleList.add(each_val);
                    } else {

                        // tenant_user_res
                        boolean isRes = each_val.containsColumn(CF, RESOURCE_ID);
                        if (isRes) {
                            resList.add(each_val);
                        } else {
                            // u8-user
                            boolean isU8 = each_val.containsColumn(CF, U8_USERNAME);
                            if (isU8) {
                                u8List.add(each_val);
                            } else {
                                domainList.add(each_val);
                            }
                        }
                    }
                }
            }
        }
        // 如果没有租户或者用户，跳过
        if (tenantUserRelaResult != null) {
            Put put = new Put(tenantUserRelaResult.getRow());
            for (Cell cell : tenantUserRelaResult.listCells()) {
                put.addColumn(CF, cell.getQualifier(), cell.getValue());
            }

            // add resource info
            StringBuilder resIdsBuilder = new StringBuilder();
            StringBuilder resCodesBuilder = new StringBuilder();
            for (int j = 0; j < resList.size(); j++) {
                resIdsBuilder.append(Bytes.toString(resList.get(j).getValue(CF, RESOURCE_ID))).append(",");
                resCodesBuilder.append(Bytes.toString(resList.get(j).getValue(CF, RESOURCE_CODE))).append(",");
            }
            put.addColumn(CF, RESOURCE_ID, Bytes.toBytes(resIdsBuilder.toString()));
            put.addColumn(CF, RESOURCE_CODE, Bytes.toBytes(resCodesBuilder.toString()));

            // add role info
            StringBuilder roleIdBuffer = new StringBuilder();
            StringBuilder roleCodeBuffer = new StringBuilder();
            for (int k = 0; k < roleList.size(); k++) {
                roleIdBuffer.append(Bytes.toString(roleList.get(k).getValue(CF, ROLE_ID))).append(",");
                roleCodeBuffer.append(Bytes.toString(roleList.get(k).getValue(CF, ROLE_CODE))).append(",");
            }
            put.addColumn(CF, ROLE_ID, Bytes.toBytes(roleIdBuffer.toString()));
            put.addColumn(CF, ROLE_CODE, Bytes.toBytes(roleCodeBuffer.toString()));

            // add u8 info
            StringBuilder u8NameBuilder = new StringBuilder();
            StringBuilder u8CodeBuilder = new StringBuilder();
            for (int i = 0; i < u8List.size(); i++) {
                u8NameBuilder.append(Bytes.toString(u8List.get(i).getValue(CF, U8_USERNAME))).append(",");
                u8CodeBuilder.append(Bytes.toString(u8List.get(i).getValue(CF, U8_USERCODE))).append(",");
            }
            put.addColumn(CF, U8_USERNAME, Bytes.toBytes(u8NameBuilder.toString()));
            put.addColumn(CF, U8_USERCODE, Bytes.toBytes(u8CodeBuilder.toString()));

            // add domainInfo
            StringBuilder domainBuilder = new StringBuilder();
            for (int i = 0; i < domainList.size(); i++) {
                domainBuilder.append(Bytes.toString(domainList.get(i).getValue(CF, DOMAIN_ID))).append(",");
            }
            put.addColumn(CF, DOMAIN_ID, Bytes.toBytes(domainBuilder.toString()));
            context.write(key, put);
        }

    }
}
