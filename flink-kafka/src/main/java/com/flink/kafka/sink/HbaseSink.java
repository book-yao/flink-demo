package com.flink.kafka.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;

public class HbaseSink extends RichSinkFunction<Tuple2<String, Long>> {
    private Configuration conf;
    private Connection conn;

    private String[] array = {"1001", "1002", "1003", "1004", "1005"};

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.client.retries.number", "3");
        conf.set("zookeeper.recovery.retry", "3");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("java.security.krb5.conf", "C:\\opt\\data\\kerberos\\krb5.conf");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@RCMD.COM");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@RCMD.COM");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hbase/cdh01@HS.COM", "C:\\opt\\data\\kerberos\\hbase.keytab");
        try {
            FileSystem.get(conf);
            conn = ConnectionFactory.createConnection(conf, Executors.newFixedThreadPool(10));
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (conf != null) {
            conf.clear();
        }

        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf("test"));
            String rowKey = array[new Random().nextInt(5)];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("key"), Bytes.toBytes(value.f0));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(value.f1));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
}
