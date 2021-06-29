package com.flink.hbase.reader;

import com.flink.hbase.hbase.HbaseUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * Hbase数据源
 * @author JWF
 * @date 2021-6-29
 */
public class HBaseSetResource extends RichSourceFunction<Tuple2<String,String>> {

    private Table table;

    @Override
    public void run(SourceContext<Tuple2<String,String>> sourceContext) throws Exception {
        table = HbaseUtils.getConnection().getTable(TableName.valueOf("test"));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("key"));

        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1006"));

        final ResultScanner scanner = table.getScanner(scan);
        final Iterator<Result> iterator = scanner.iterator();

        while(iterator.hasNext()){
            final Result next = iterator.next();

            final String rowKey = Bytes.toString(next.getRow());
            final String keyValue = Bytes.toString(next.getValue(Bytes.toBytes("cf"), Bytes.toBytes("key")));

            sourceContext.collect(new Tuple2<>(rowKey,keyValue));
        }
    }

    @Override
    public void cancel() {
        if(table != null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
