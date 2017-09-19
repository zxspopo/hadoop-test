package com.yonyou.zxs.mapreduce.honor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zengxs on 2017/5/17.
 */
public class HonorMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static final byte[] CF = Bytes.toBytes("cf");
    public static final byte[] FROM_MEMBER_ID = Bytes.toBytes("from_member_id");
    public static final byte[] TO_OWNER_ID = Bytes.toBytes("to_owner_id");
    public static final byte[] NAME = Bytes.toBytes("name");
    public static final byte[] USER_NAME = Bytes.toBytes("user_name");
    public static final byte[] FROM_NAME = Bytes.toBytes("from_name");
    public static final byte[] FROM_U_NAME = Bytes.toBytes("from_user_name");
    public static final byte[] TO_NAME = Bytes.toBytes("to_name");
    public static final byte[] TO_USER_NAME = Bytes.toBytes("to_user_name");
    public static final byte[] TAG_NAME = Bytes.toBytes("tag_name");
    public static final byte[] TAG_ID = Bytes.toBytes("honour_tag_id");

    private static final Map<String, byte[]> resultMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        HTable table = new HTable(context.getConfiguration(), context.getConfiguration().get("hongbao_honor_tag"));
        Scan scan = new Scan();
        scan.setMaxVersions();
        Scan sc = new Scan();
        sc.setCaching(10000);
        sc.setCacheBlocks(false);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            resultMap.put(Bytes.toString(r.getRow()), r.getValue(CF, NAME));
        }
        rs.close();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result res, Context context) throws IOException,
            InterruptedException {
        if (res == null || res.isEmpty())
            return;
        res.getFamilyMap(CF);

        byte[] fromMemberId = res.getValue(CF, FROM_MEMBER_ID);
        byte[] toMemberId = res.getValue(CF, TO_OWNER_ID);
        Result fromMemberRs = getMember(fromMemberId, context.getConfiguration());
        Result toMemberRs = getMember(toMemberId, context.getConfiguration());
        if (fromMemberRs != null && toMemberRs != null) {
            byte[] fromName = fromMemberRs.getValue(CF, NAME);
            byte[] fromUsername = fromMemberRs.getValue(CF, USER_NAME);

            byte[] toName = toMemberRs.getValue(CF, NAME);
            byte[] toUsername = toMemberRs.getValue(CF, USER_NAME);

            Put put = new Put(res.getRow());

            for (KeyValue kv : res.list()) {
                put.add(kv);
            }
            String tagId = Bytes.toString(res.getValue(CF, TAG_ID));
            put.addColumn(CF, FROM_NAME, fromName);
            put.addColumn(CF, FROM_U_NAME, fromUsername);
            put.addColumn(CF, TO_NAME, toName);
            put.addColumn(CF, TO_USER_NAME, toUsername);
            put.addColumn(CF, TAG_NAME, resultMap.get(tagId));
            context.write(key, put);
        }

    }

    // conf.set("hbase.zookeeper.quorum", context.getConfiguration().get("hbase.zookeeper.quorum"));
    private Result getMember(byte[] member_id, Configuration conf) throws IOException {
        HTable table = new HTable(conf, conf.get("member_table_name"));
        if (member_id != null && member_id.length > 0) {
            Get g = new Get(member_id);
            Result r = table.get(g);
            return r;
        } else {
            return null;
        }
    }
}
