package com.example.back.entities;

public class Sensor {
    String sensorType;
    int in;
    int out;

    public Sensor(String sensorType, int in, int out) {
        this.sensorType = sensorType;
        this.in = in;
        this.out = out;
    }
}
