package Prototype;

import Camera.CameraT1;
import Camera.CameraT2;
import Radar.RadarTagmaster1;
import Radar.RadarTagmaster2;
import Radar.RadarViking;
import Tube.Tube;

import java.util.Map;

public class DataCleanerFactory {

    private static final Map<String, DataCleaner> cleaners = Map.of(
            "RADAR_VIKING", new RadarViking(),
            "RADAR_TAGMASTER1", new RadarTagmaster1(),
            "RADAR_TAGMASTER2", new RadarTagmaster2(),
            "CAMERA_T1", new CameraT1(),
            "CAMERA_T2", new CameraT2(),
            "TUBE_MIXTRA", new Tube()
    );

    public static DataCleaner getCleaner(String type) {
        DataCleaner cleaner = cleaners.get(type);
        if (cleaner != null) {
            return cleaners.get(type);
        } else {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }
}