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
        Path filePath = ((FileSplit) split).getPath();
        String filePathString = filePath.toString();
        String type = Helper.getSensorTypeFromPostName(filePathString);
        fileName = filePath.getName();
        String line = value.toString();
        Cleaner cleaner = CleanerFactory.getCleaner(type);
        if (cleaner.isValid(line)) {
            context.write(NullWritable.get(), new Text(cleaner.adapt(line, filePathString)));
        }
    }
}