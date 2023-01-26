import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageSpeedPerHour implements Analyze{

    public Text getValue(Iterable<Text> values){
        float sum = 0.0f;
        int count = 0;
        for (Text t : values) {
            String[] tokens = t.toString().split(",");
            sum += Integer.parseInt(tokens[6]) ;
            count ++;
        }
        sum = sum / count;
        return new Text(sum+"");
    }

    public Text map(Text value){
        String line = value.toString();
        String[] tokens = line.split(",");
        return new Text(tokens[5].split(":")[0]);
    }

    @Override
    public boolean isValidline() {
        return true;
    }
}
