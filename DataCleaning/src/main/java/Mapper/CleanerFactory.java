class CleanerFactory {

    private final Map<String, Cleaner> cleaners = Map.of(
            "RADAR_VIKING", new RadarViking(),
            "RADAR_TAGMASTER1", new RadarTagmaster1(),
            "RADAR_TAGMASTER2", new RadarTagmaster2(),
            "CAMERA_T1", new CameraT1(),
            "CAMERA_T2", new CameraT2(),
            "TUBE_MIXTRA", new Tube()
    );

    public static Cleaner getCleaner(String type) {
        Cleaner cleaner = cleaners.get(type);
        if (cleaner != null) {
            return cleaners.get(type);
        } else {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }
}