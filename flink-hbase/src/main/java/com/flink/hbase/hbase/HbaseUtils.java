package com.flink.hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.Executors;

public class HbaseUtils {
    private static Configuration conf;
    private static Connection conn;

    static {
        try {
            open();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Connection getConnection(){
        return conn;
    }

    public static void open() throws Exception{
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

    public void close() throws IOException {
        if (conf != null) {
            conf.clear();
        }

        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
