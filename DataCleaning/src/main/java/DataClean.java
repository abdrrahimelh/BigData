

import Helper.Helper;
import Helper.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataClean {
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.set("fileName",args[0]) ;
    String type = Helper.getSensorTypeFromPostName(args[0]);
    Job job = Job.getInstance(conf, type);
    job.setNumReduceTasks(0);
    job.setJarByClass(DataClean.class);
    job.setMapperClass(Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    for (String str : Input.strings) {
      FileInputFormat.addInputPath(job, new Path(Helper.adaptInput(str)));
    }
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
