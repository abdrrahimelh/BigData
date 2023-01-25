class CleanerFactory {
    public static Cleaner getCleaner(String type) {
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