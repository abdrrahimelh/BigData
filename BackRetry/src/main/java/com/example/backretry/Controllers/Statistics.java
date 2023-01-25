package com.example.backretry.Controllers;


import com.example.backretry.services.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/stats")
public class Statistics {

    @Autowired
    HBaseService hbaseService;

    @GetMapping("/table")
    public ResponseEntity <List<String>> getTableData() throws IOException {
        List<String> data = hbaseService.getData("helfani:test_sensor");
        HttpHeaders headers = new HttpHeaders();
        return ResponseEntity.accepted().headers(headers).body(data);
    }
}
