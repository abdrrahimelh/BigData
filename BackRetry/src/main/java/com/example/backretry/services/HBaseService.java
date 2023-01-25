package com.example.backretry.services;

//import com.example.back.configuration.HBaseConfig;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HBaseService {


    @Autowired
    private Connection hbaseConnection;

    public List<String> getData(String tableName) throws IOException {
        List<String> data = new ArrayList<>();
        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            data.add(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col"))));
        }
        scanner.close();
        table.close();
        return data;
    }


    //other methods
}