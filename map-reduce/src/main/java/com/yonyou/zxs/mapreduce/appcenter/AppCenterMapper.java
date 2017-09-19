package com.yonyou.zxs.mapreduce.appcenter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zengxs on 2017/5/17.
 */
public class AppCenterMapper extends TableMapper<ImmutableBytesWritable, Put> {

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
    public static final byte[] RESOURCE_ID = Bytes.toBytes("res_id");
    public static final byte[] RESOURCE_CODE = Bytes.toBytes("res_code");

    private LRUCache<String, Result> cache = new LRUCache<>(3000);

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) {
        try {
            if (res == null || res.isEmpty())
                return;
            res.getFamilyMap(CF);
            byte[] tenantId = res.getValue(CF, TENANT_ID);
            byte[] userId = res.getValue(CF, USER_ID);

            Result userInfo = findUserInfo(userId, context.getConfiguration());
            Result tenantInfo = findTenantInfo(tenantId, context.getConfiguration());
            Map<String, String> resCodeAndIdMap = findResInfo(tenantId, userId, context.getConfiguration());

            // 如果没有租户或者用户，跳过
            if (!(userInfo == null || userInfo.isEmpty() || tenantInfo == null || tenantInfo.isEmpty())) {
                Put put = new Put(Bytes.toBytes(Bytes.toString(tenantId) + ":" + Bytes.toString(userId)));
                put.addColumn(CF, TENANT_NAME, tenantInfo.getValue(CF, TENANT_NAME));
                put.addColumn(CF, TENANT_CODE, tenantInfo.getValue(CF, TENANT_CODE));
                put.addColumn(CF, TENANT_FULLNAME, tenantInfo.getValue(CF, TENANT_FULLNAME));
                put.addColumn(CF, USER_EMAIL, userInfo.getValue(CF, USER_EMAIL));
                put.addColumn(CF, USER_NAME, userInfo.getValue(CF, USER_NAME));
                put.addColumn(CF, VALIDATE_MOBILE, userInfo.getValue(CF, VALIDATE_MOBILE));
                put.addColumn(CF, SYSTEM_ID, userInfo.getValue(CF, SYSTEM_ID));
                put.addColumn(CF, VERIFIED, userInfo.getValue(CF, VERIFIED));
                put.addColumn(CF, VALIDATE_EMAIL, userInfo.getValue(CF, VALIDATE_EMAIL));
                put.addColumn(CF, REGISTER_DATE, userInfo.getValue(CF, REGISTER_DATE));
                put.addColumn(CF, NEED_MERGE, userInfo.getValue(CF, NEED_MERGE));
                put.addColumn(CF, USER_AVATOR, userInfo.getValue(CF, USER_AVATOR));
                put.addColumn(CF, USER_CODE, userInfo.getValue(CF, USER_CODE));
                put.addColumn(CF, USER_ID, userId);
                put.addColumn(CF, USER_MOBILE, userInfo.getValue(CF, USER_MOBILE));
                put.addColumn(CF, USER_AVATOR_NEW, userInfo.getValue(CF, USER_AVATOR_NEW));
                put.addColumn(CF, TYPE_ID, res.getValue(CF, TYPE_ID));
                put.addColumn(CF, TENANT_ID, tenantId);
                put.addColumn(CF, RESOURCE_ID, Bytes.toBytes(resCodeAndIdMap.get("idArray")));
                put.addColumn(CF, RESOURCE_CODE, Bytes.toBytes(resCodeAndIdMap.get("codeArray")));
                context.write(key, put);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> findResInfo(byte[] tenantId, byte[] userId, Configuration conf) throws IOException {
        HTable table = new HTable(conf, conf.get("tenant.res.table.name"));
        RowFilter rowFilter =
                new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(Bytes.toString(tenantId) + ":"
                        + Bytes.toString(userId) + ".*"));
        Scan scan = new Scan();
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> codeAndIdMap = new HashMap<>();
        StringBuilder idBuilder = new StringBuilder();
        StringBuilder codeBuilder = new StringBuilder();
        for (Result r : rs) {
            idBuilder.append(Bytes.toString(r.getValue(CF, RESOURCE_ID))).append(",");
            codeBuilder.append(Bytes.toString(r.getValue(CF, RESOURCE_CODE))).append(",");
        }
        codeAndIdMap.put("idArray", idBuilder.toString());
        codeAndIdMap.put("codeArray", idBuilder.toString());
        return codeAndIdMap;

    }

    private Result findUserInfo(byte[] userId, Configuration conf) throws IOException {

        if (userId == null || userId.length == 0) {
            return null;
        }
        String userIdStr = Bytes.toString(userId);
        Result result = cache.get(userIdStr);
        if (result != null) {
            return result;
        } else {
            HTable table = new HTable(conf, conf.get("tenant.user.table.name"));
            Get g = new Get(userId);
            Result res = table.get(g);
            if (res.isEmpty()) {
                table = new HTable(conf, "yht_app_user");
                g = new Get(userId);
                res = table.get(g);
            }
            if (!res.isEmpty()) {
                cache.put(userIdStr, res);
            }
            return res;
        }

    }

    private Result findTenantInfo(byte[] tenantId, Configuration conf) throws IOException, InterruptedException {
        if (tenantId == null || tenantId.length == 0) {
            return null;
        }
        HTable table = new HTable(conf, conf.get("tenant.table.name"));
        Get g = new Get(tenantId);
        Result res = table.get(g);
        return res;

    }
}
