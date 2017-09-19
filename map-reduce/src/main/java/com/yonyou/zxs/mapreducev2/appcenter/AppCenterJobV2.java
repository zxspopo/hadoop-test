package com.yonyou.zxs.mapreducev2.appcenter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zengxs on 2017/7/1.
 */
public class AppCenterJobV2 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("hbase.zookeeper.quorum", args[0]);
        conf.set("tenant.table.name", "market_pub_tenant");
        conf.set("tenant.user.table.name", "market_pub_tenant_user");
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.1");
        // conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 6000000);
        // conf.setInt("mapreduce.task.timeout", 1800000);
        // conf.set(
        // "mapred.child.java.opts",
        // "-XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:GCTimeRatio=10 -XX:YoungGenerationSizeIncrement=20 -XX:TenuredGenerationSizeIncrement=20 -XX:AdaptiveSizeDecrementScaleFactor=2 -Xmx2000m");
        Job job = createSubmittableJob(conf, args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static Job createSubmittableJob(Configuration conf, String dstTable) throws IOException,
            ClassNotFoundException, InterruptedException {
        String jobName = "appcenter-tenant-resource-job";
        String relaTable = "tenant_user_rela";
        String resTable = "tenant_user_res";
        String tenantTable = "market_pub_tenant";
        String userTable = "market_pub_tenant_user";
        String yhtUserTable = "yht_app_user";
        String u8UserTable = "pub_u8_user";
        String domainTable = "pub_tenant_domain_permission";
        String tenantUserTable = "tenant_user_tmp";
        String tenantRelaTmp = "tenant_rela_tmp";
        String tenantRoleTmp = "tenant_role_tmp";
        String tenantRoleTable = "tenant_role_info";
        String roleUserTable = "tenant_role_user";
        truncateTable(tenantUserTable, conf);
        truncateTable(tenantRelaTmp, conf);
        truncateTable(tenantRoleTmp, conf);

        // tenant-rela
        runTenantRelaJob(conf, relaTable, tenantTable, tenantRelaTmp);

        // tenant-rela-user
        runRelaUserJob(conf, userTable, yhtUserTable, tenantUserTable, tenantRelaTmp);

        // tenant-role
        runRoleUserJob(conf, tenantRoleTmp, tenantRoleTable, roleUserTable);

        // tenat-rela-user-res
        Job job =
                runFinalJob(conf, jobName, resTable, dstTable, tenantUserTable, tenantRoleTmp, u8UserTable, domainTable);
        return job;
    }

    private static void runRoleUserJob(Configuration conf, String tenantRoleTmp, String tenantRoleTable,
            String roleUserTable) throws IOException, InterruptedException, ClassNotFoundException {
        List<Scan> relaScanList = new ArrayList<>();
        Scan tenantScan = new Scan();
        tenantScan.setCaching(10000);
        tenantScan.setCacheBlocks(false);
        tenantScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tenantRoleTable.getBytes());
        relaScanList.add(tenantScan);
        Scan relaScan = new Scan();
        relaScan.setCaching(10000);
        relaScan.setCacheBlocks(false);
        relaScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, roleUserTable.getBytes());
        relaScanList.add(relaScan);
        Job relaJob = new Job(conf, "tenant-role-job");
        relaJob.setJarByClass(AppCenterJobV2.class);
        relaJob.setMapperClass(TenantUserRoleMapper.class);
        relaJob.setReducerClass(TenantUserRoleReduce.class);
        TableMapReduceUtil.initTableMapperJob(relaScanList, TenantUserRoleMapper.class, ImmutableBytesWritable.class,
                Result.class, relaJob);
        TableMapReduceUtil.initTableReducerJob(tenantRoleTmp, TenantUserRoleReduce.class, relaJob);
        relaJob.waitForCompletion(true);
    }

    private static Job runFinalJob(Configuration conf, String jobName, String resTable, String dstTable,
            String tenantUserTable, String tenantRoleTmpTable, String u8Table, String domainTable) throws IOException {
        List<Scan> scanList = new ArrayList<>();
        Scan tenantUserTmpScan = new Scan();
        tenantUserTmpScan.setCaching(10000);
        tenantUserTmpScan.setCacheBlocks(false);
        tenantUserTmpScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tenantUserTable.getBytes());
        scanList.add(tenantUserTmpScan);
        Scan resScan = new Scan();
        resScan.setCaching(10000);
        resScan.setCacheBlocks(false);
        resScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, resTable.getBytes());
        scanList.add(resScan);
        Scan roleScan = new Scan();
        roleScan.setCaching(10000);
        roleScan.setCacheBlocks(false);
        roleScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tenantRoleTmpTable.getBytes());
        scanList.add(roleScan);
        // tenant-user-u8
//        Scan u8Scan = new Scan();
//        u8Scan.setCaching(10000);
//        u8Scan.setCacheBlocks(false);
//        u8Scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, u8Table.getBytes());
//        scanList.add(u8Scan);
//        // tenant-user-domains
//        Scan domainScan = new Scan();
//        domainScan.setCaching(10000);
//        domainScan.setCacheBlocks(false);
//        domainScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, domainTable.getBytes());
//        scanList.add(domainScan);

        Job job = new Job(conf, jobName);
        job.setJarByClass(AppCenterJobV2.class);
        job.setMapperClass(AppCenterMapperV2.class);
        job.setReducerClass(AppCenterReduce.class);
        TableMapReduceUtil.initTableMapperJob(scanList, AppCenterMapperV2.class, ImmutableBytesWritable.class,
                Result.class, job);
        TableMapReduceUtil.initTableReducerJob(dstTable, AppCenterReduce.class, job);
        return job;
    }

    private static void runRelaUserJob(Configuration conf, String userTable, String yhtUserTable,
            String tenantUserTable, String tenantRelaTmp) throws IOException, InterruptedException,
            ClassNotFoundException {
        List<Scan> userScanList = new ArrayList<>();
        Scan yhtScan = new Scan();
        yhtScan.setCaching(10000);
        yhtScan.setCacheBlocks(false);
        yhtScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, yhtUserTable.getBytes());
        userScanList.add(yhtScan);
        Scan userScan = new Scan();
        userScan.setCaching(10000);
        userScan.setCacheBlocks(false);
        userScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, userTable.getBytes());
        userScanList.add(userScan);
        Scan relaTmpScan = new Scan();
        relaTmpScan.setCaching(10000);
        relaTmpScan.setCacheBlocks(false);
        relaTmpScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tenantRelaTmp.getBytes());
        userScanList.add(relaTmpScan);
        Job userJob = new Job(conf, "tenant-user-job");
        userJob.setJarByClass(AppCenterJobV2.class);
        userJob.setMapperClass(TenantUserMapper.class);
        userJob.setReducerClass(TenantUserReduce.class);
        TableMapReduceUtil.initTableMapperJob(userScanList, TenantUserMapper.class, ImmutableBytesWritable.class,
                Result.class, userJob);
        TableMapReduceUtil.initTableReducerJob(tenantUserTable, TenantUserReduce.class, userJob);
        userJob.waitForCompletion(true);
    }

    private static void runTenantRelaJob(Configuration conf, String relaTable, String tenantTable, String tenantRelaTmp)
            throws IOException, InterruptedException, ClassNotFoundException {
        List<Scan> relaScanList = new ArrayList<>();
        Scan tenantScan = new Scan();
        tenantScan.setCaching(10000);
        tenantScan.setCacheBlocks(false);
        tenantScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tenantTable.getBytes());
        relaScanList.add(tenantScan);
        Scan relaScan = new Scan();
        relaScan.setCaching(10000);
        relaScan.setCacheBlocks(false);
        relaScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, relaTable.getBytes());
        relaScanList.add(relaScan);
        Job relaJob = new Job(conf, "tenant-rela-job");
        relaJob.setJarByClass(AppCenterJobV2.class);
        relaJob.setMapperClass(TenantRelaMapper.class);
        relaJob.setReducerClass(TenantRelaReduce.class);
        TableMapReduceUtil.initTableMapperJob(relaScanList, TenantRelaMapper.class, ImmutableBytesWritable.class,
                Result.class, relaJob);
        TableMapReduceUtil.initTableReducerJob(tenantRelaTmp, TenantRelaReduce.class, relaJob);
        relaJob.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AppCenterJobV2(), args);
        System.exit(exitCode);
    }

    private static void truncateTable(String tableName, Configuration configuration) throws IOException {
        // 首先要获得table的建表信息
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor td = admin.getTableDescriptor(Bytes.toBytes(tableName));

        // 删除表
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        // 重新建表
        admin.createTable(td);
    }
}
