import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountPerDay implements Analyze{

    public  Text getValue(Iterable<Text> values){
        int entrees = 0;
        int sorties = 0;
        for (Text t : values) {
            String[] tokens = t.toString().split(",");
            if(tokens[2].equals("1")){
                entrees++;
            }
            else {
                sorties++;
            }
        }
        return new Text(entrees+","+sorties);
    }

    public  Text map(Text value){
        String line = value.toString();
        String[] tokens = line.split(",");
        return new Text(tokens[3]);
    }

    public boolean isValidline(){
        return true;
    }
}
