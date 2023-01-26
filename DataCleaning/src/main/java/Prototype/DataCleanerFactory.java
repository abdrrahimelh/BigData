package Prototype;

import Camera.CameraT1;
import Camera.CameraT2;
import Radar.RadarTagmaster1;
import Radar.RadarTagmaster2;
import Radar.RadarViking;
import Tube.Tube;

public class DataCleanerFactory {
    public static DataCleaner getCleaner(String type) {
        switch (type) {
            case "RADAR_VIKING":
                return new RadarViking();
            case "RADAR_TAGMASTER1":
                return new RadarTagmaster1();
            case "RADAR_TAGMASTER2":
                return new RadarTagmaster2();
            case "CAMERA_T1":
                return new CameraT1();
            case "CAMERA_T2":
                return new CameraT2();
            case "TUBE_MIXTRA":
                return new Tube();
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
    }
}