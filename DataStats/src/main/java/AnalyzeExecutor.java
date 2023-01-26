import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Objects;

public class AnalyzeExecutor {

    public static Analyze getAnalyzerFromType(String analyze_type){
        if( Objects.equals(analyze_type, "in_out_per_day")){
            return new CountPerDay();
        }
        else if(Objects.equals(analyze_type, "in_out_per_hour")){
            return new CountPerHour();
        }
        else if(Objects.equals(analyze_type, "in_out_per_sensor_hour")){
            return new CountPerSensorHour();
        }
        else if(Objects.equals(analyze_type, "speed_per_hour")){
            return new AverageSpeedPerHour();
        }
        return null;
    }

    public static class AnalyzeMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf =context.getConfiguration();
            String analyze_type = conf.get("analyze_type");
            Analyze analyze = getAnalyzerFromType(analyze_type);
            if(analyze != null){
                Text key_out = analyze.map(value);
                if (analyze.isValidline()) {
                    context.write(key_out, value);
                }
            }
        }
    }

    public static class AnalyzeReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Configuration conf =context.getConfiguration();
            String analyze_type = conf.get("analyze_type");
            Analyze analyze = getAnalyzerFromType(analyze_type);
            if(analyze != null){
                Text value = analyze.getValue(values);
                context.write(key, value);
            }
        }
    }
}
