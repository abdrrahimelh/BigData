import org.apache.hadoop.io.Text;

public interface Analyze{

    public  Text getValue(Iterable<Text> values);

    public  Text map(Text value);

    public boolean isValidline();
}
