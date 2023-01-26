import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TestAnalyze {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "TestAnalyze");

        job.setNumReduceTasks(1);


        job.setJarByClass(CountPerDay.class);
        job.setMapperClass(CountPerDay.TestAnalyzeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CountPerDay.TestAnalyzeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        /*
        Job job1 = Job.getInstance(conf, "3alamkhor");
        job1.setNumReduceTasks(2);

        job1.setJarByClass(TestAnalyze.class);
        job1.setMapperClass(CountPerHour.TestAnalyzeMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(CountPerHour.TestAnalyzeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Integer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Count difference ");
        job2.setNumReduceTasks(2);

        job2.setJarByClass(TestAnalyze.class);
        job2.setMapperClass(CountPerCapteurHour.TestAnalyzeMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(CountPerCapteurHour.TestAnalyzeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.waitForCompletion(true);*/
    }
}