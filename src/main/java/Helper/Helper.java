package Helper;

import Camera.CameraT1;
import Camera.CameraT2;
import Radar.RadarTagmaster1;
import Radar.RadarTagmaster2;
import Radar.RadarViking;
import Tube.Tube;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Arrays;
import java.util.List;
public class Helper {

    public static String getDirectionsRadar(String filePath){
        String[] file = filePath.split("/");
        String fileName = file[file.length - 1];
        String[] direction = fileName.split("_");
        if (direction[1].startsWith("Sort")) return "2";
        return "1";
    }

    public static int getDirectionsTube(String fileName){
        String filePath[] = fileName.split("/") ;
        String direction = filePath[filePath.length - 1];
        Map<String, Integer> directions = new HashMap<>();
        directions.put("P23_Vers_COSEC.csv",2);
        directions.put("P23_Vers_BEC_1.csv",1);
        directions.put("P23_Vers_BEC_2.csv",1);
        directions.put("P24_Vers_Fac.csv", 1);
        directions.put("P24_Vers_Rocade.csv", 2);
        directions.put("P26_Vers_Fac_1.csv",1 );
        directions.put("P26_Vers_Fac_2.csv", 1);
        directions.put("P26_Vers_Fac_3.csv", 1);
        directions.put("P26_Vers_Rocade_1.csv",2 );
        directions.put("P26_Vers_Rocade_2.csv",2 );
        directions.put("P19_Entree.csv",1);
        directions.put("P19_Sortie.csv",2 );
        directions.put("P9_Vers_Fac_1.csv",1);
        directions.put("P9_Vers_Fac_2.csv",1);
        directions.put("P9_Vers_Talence_1.csv",2);
        directions.put("P9_Vers_Talence_2.csv",2);
        return directions.get(direction);
    }

    public static String getSensorTypeFromPostName(String file) {

        int postId;
        String[] filePath = file.split("/");
        String fileName = filePath[filePath.length - 2];
        // Use regular expression to match the pattern of file name
        Pattern pattern = Pattern.compile("P(\\d+)");
        Matcher matcher = pattern.matcher(fileName);

        // Check if the file name matches the pattern
        if (matcher.matches()) {
            // Get the number from the file name
            postId = Integer.parseInt(matcher.group(1));
        } else {
            // Throw an exception if the file name does not match the pattern
            throw new IllegalArgumentException("Invalid file name: " + fileName);
        }

        List<Integer> tube_mixtra = Arrays.asList(9, 11, 19, 23, 24, 26);
        List<Integer> tube_delta = Arrays.asList(15);
        List<Integer> rada_viking = Arrays.asList(4, 5, 17);
        List<Integer> radar_tagmaster1 = Arrays.asList(3);
        List<Integer> radar_tagmaster2 = Arrays.asList(2);
        List<Integer> camera_1 = Arrays.asList(1, 6, 7, 8, 10, 12, 13, 14, 16, 18, 21, 22, 25, 27);
        List<Integer> camera_2 = Arrays.asList(20);

        if (tube_mixtra.contains(postId)) {
            return "TUBE_MIXTRA";
        } else if (tube_delta.contains(postId)) {
            return "TUBE_DELTA";
        } else if (rada_viking.contains(postId)) {
            return "RADAR_VIKING";
        } else if (radar_tagmaster1.contains(postId)) {
            return "RADAR_TAGMASTER1";
        } else if (radar_tagmaster2.contains(postId)) {
            return "RADAR_TAGMASTER2";
        } else if (camera_1.contains(postId)) {
            return "CAMERA_T1";
        } else if (camera_2.contains(postId)) {
            return "CAMERA_T2";
        } else {
            // Throw an exception if the post id does doesn't belong to [1-27]
            throw new IllegalArgumentException("Invalid post id : " + postId);
        }
    }

    public static Class<?> getClassFromType(String type) {
        if (type.equals("RADAR_VIKING")) {
            return RadarViking.class;
        }
        else if (type.equals("RADAR_TAGMASTER1")) {
            return RadarTagmaster1.class;
        }
        else if (type.equals("RADAR_TAGMASTER2")) {
            return RadarTagmaster2.class;
        }
        else if (type.equals("CAMERA_T1")) {
            return CameraT1.class;
        }
        else if (type.equals("CAMERA_T2")) {
            return CameraT2.class;
        }
        else if (type.equals("TUBE_MIXTRA")) {
            return Tube.class;
        }
        else {
            throw new IllegalArgumentException("Invalid type : " + type);
        }
    }

    public static Class<? extends Mapper> getMapperFromType(String type) {
        if (type.equals("RADAR_VIKING")) {
            return RadarViking.RadarMapper.class;
        }
        else if (type.equals("RADAR_TAGMASTER1")) {
            return RadarTagmaster1.RadarMapper.class;
        }
        else if (type.equals("RADAR_TAGMASTER2")) {
            return RadarTagmaster2.RadarMapper.class;
        }
        else if (type.equals("CAMERA_T1")) {
            return CameraT1.CamMapper.class;
        }
        else if (type.equals("CAMERA_T2")) {
            return CameraT2.CamMapper.class;
        }
        else if (type.equals("TUBE_MIXTRA")) {
            return Tube.TubeMapper.class;
        }
        else {
            throw new IllegalArgumentException("Invalid type : " + type);
        }
    }

    public static String getPosition(String filePath){
        String[] file = filePath.split("/");
        String fileName = file[file.length - 2];
        return fileName;
    }
}
