package com.flink.hbase.sink;

import com.flink.hbase.hbase.HbaseUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

/**
 * Hbase存储
 * @author JWF
 * @date 2021-6-29
 */
public class HbaseSink extends RichSinkFunction<Tuple2<String, Long>> {
    private Configuration conf;
    private Connection conn;

    private String[] array = {"1001", "1002", "1003", "1004", "1005"};

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        Table table = null;
        try {
            table = HbaseUtils.getConnection().getTable(TableName.valueOf("test"));
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
