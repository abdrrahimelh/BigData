import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageSpeedPerHour {
    public static class TestAnalyzeMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            if (true) { // if (isValid(line))
                context.write(new Text(tokens[5].split(":")[0]), value);
            }
        }
    }

    public static class TestAnalyzeReducer
            extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (Text t : values) {
                String[] tokens = t.toString().split(",");
                sum += Integer.parseInt(tokens[6]) ;
                count ++;
            }
            sum = sum / count;
            context.write(key, new FloatWritable(sum));
        }
    }
}
