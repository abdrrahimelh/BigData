package com.example.back.services;

import com.example.back.configuration.HBaseConfig;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HBaseService {
    private final Admin hbaseAdmin;

    public HBaseService(Admin hbaseAdmin) {
        this.hbaseAdmin = hbaseAdmin;
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (!hbaseAdmin.tableExists(tn)) {
            HTableDescriptor desc = new HTableDescriptor(tn);
            desc.addFamily(new HColumnDescriptor(columnFamily));
            hbaseAdmin.createTable(desc);
        }
    }
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