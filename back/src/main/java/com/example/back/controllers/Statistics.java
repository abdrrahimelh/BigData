package com.example.back.controllers;

import com.example.back.entities.User;
import com.example.back.services.HBaseService;
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

    private final HBaseService hbaseService;
    public Statistics(HBaseService hbaseService) {
        this.hbaseService = hbaseService;
    }

    @GetMapping("/users")
    public ResponseEntity <List< User >> users() {
        List <User> users = new ArrayList< >();
        users.add(new User(1, "Ramesh"));
        users.add(new User(2, "Tony"));
        users.add(new User(3, "Tom"));
        HttpHeaders headers = new HttpHeaders();
        headers.add("Responded", "UserController");
        return ResponseEntity.accepted().headers(headers).body(users);
    }

    @GetMapping("/table")
    public List<String> getTableData() throws IOException {
        List<String> data = hbaseService.getData("helfani:test_sensor");
        return data;
    }
}
