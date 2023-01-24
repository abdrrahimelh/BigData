package com.example.back.configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HBaseConfig {
    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfig() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        return conf;
    }

    @Bean
    public Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(hbaseConfig());
    }

    @Bean
    public Admin hbaseAdmin() throws IOException {
        return hbaseConnection().getAdmin();
    }
}
