import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountPerCapteurHour {

    public static class TestAnalyzeMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            if (true) { // if (isValid(line))
                context.write(new Text(tokens[0] + "," + tokens[5].split(":")[0]), value);
            }
        }
    }

    public static class TestAnalyzeReducer
            extends Reducer<Text, Text, Text, Integer> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text t : values) {
                String[] tokens = t.toString().split(",");
                if(tokens[2].equals("1")){
                    count++;
                }
                else {
                    count--;
                }
            }
            context.write(key, count);
        }
    }
}
