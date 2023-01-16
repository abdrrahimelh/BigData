package Helper;

import Camera.CameraT1;
import Camera.CameraT2;
import Radar.RadarTagmaster;
import Radar.RadarViking;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Arrays;
import java.util.List;
public class Helper {
    private Configuration conf;
    public Helper(Configuration conf) {
        this.conf=conf;
    }
    public static String getSensorTypeFromPostName(String fileName) {

        int postId;
        // Use regular expression to match the pattern of file name
        Pattern pattern = Pattern.compile("P(\\d+)(_\\w+)*\\.csv");
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
        List<Integer> rada_tagmaster = Arrays.asList(2, 3);
        List<Integer> camera_1 = Arrays.asList(1, 6, 7, 8, 10, 12, 13, 14, 16, 18, 21, 22, 25, 27);
        List<Integer> camera_2 = Arrays.asList(20);

        if (tube_mixtra.contains(postId)) {
            return "TUBE_MIXTRA";
        } else if (tube_delta.contains(postId)) {
            return "TUBE_DELTA";
        } else if (rada_viking.contains(postId)) {
            return "RADAR_VIKING";
        } else if (rada_tagmaster.contains(postId)) {
            return "RADAR_TAGMASTER";
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
        else if (type.equals("RADAR_TAGMASTER")) {
            return RadarTagmaster.class;
        }
        else if (type.equals("CAMERA_T1")) {
            return CameraT1.class;
        }
        else if (type.equals("CAMERA_T2")) {
            return CameraT2.class;
        }
        else {
            throw new IllegalArgumentException("Invalid type : " + type);
        }
    }

    public static Class<? extends Mapper> getMapperFromType(String type) {
        if (type.equals("RADAR_VIKING")) {
            return RadarViking.RadarMapper.class;
        }
        else if (type.equals("RADAR_TAGMASTER")) {
            return RadarTagmaster.RadarMapper.class;
        }
        else if (type.equals("CAMERA_T1")) {
            return CameraT1.CamMapper.class;
        }
        else if (type.equals("CAMERA_T2")) {
            return CameraT2.CamMapper.class;
        }
        else {
            throw new IllegalArgumentException("Invalid type : " + type);
        }
    }
}
