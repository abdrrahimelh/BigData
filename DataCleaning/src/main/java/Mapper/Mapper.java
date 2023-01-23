package Mapper;

import Camera.CameraT1;
import Camera.CameraT2;
import Helper.Helper;
import Radar.RadarTagmaster1;
import Radar.RadarTagmaster2;
import Radar.RadarViking;
import Tube.Tube;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, NullWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String fileName = conf.get("fileName");
        InputSplit split = context.getInputSplit();
        Path filePath = ((FileSplit) split).getPath().getParent();
        String filePathString = filePath.toString();
        String type = Helper.getSensorTypeFromPostName(filePathString);
        fileName = filePath.getName();
        String line = value.toString();
        if (type.equals("RADAR_VIKING")) {
            if (RadarViking.isValid(line)) {
                context.write(NullWritable.get(), new Text(RadarViking.adapt(line, filePathString)));
            }
        }
        else if (type.equals("RADAR_TAGMASTER1")) {
            if (RadarTagmaster1.isValid(line)) {
                context.write(NullWritable.get(), new Text(RadarTagmaster1.adapt(line, filePathString)));
            }
        }
        else if (type.equals("RADAR_TAGMASTER2")) {
            if (RadarTagmaster2.isValid(line)) {
                context.write(NullWritable.get(), new Text(RadarTagmaster2.adapt(line, filePathString)));
            }
        }
        else if (type.equals("CAMERA_T1")) {
            if (CameraT1.isValid(line)) {
                context.write(NullWritable.get(), new Text(CameraT1.adapt(line, filePathString)));
            }
        }
        else if (type.equals("CAMERA_T2")) {
            if (CameraT2.isValid(line)) {
                context.write(NullWritable.get(), new Text(CameraT2.adapt(line, filePathString)));
            }
        }
        else if (type.equals("TUBE_MIXTRA")) {
            if (Tube.isValid(line)) {
                context.write(NullWritable.get(), new Text(Tube.adapt(line, filePathString)));
            }
        }
        else {
            throw new IllegalArgumentException("Invalid type : " + type);
        }

        // context.write(word, one);
    }

}