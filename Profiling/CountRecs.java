// Anvita Yellamanchali
// Profiling Data

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountRecs {
    public static void main(String[] args) 
		throws Exception {

        if (args.length != 2) {
			System.err.println("Usage: CountRecs <input path> <output path>");
			System.exit(-1);
		}

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Records");
        job.setJarByClass(CountRecs.class);
        job.setJobName("Count Records");
        // Setting reducer to one for only one output file
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CountRecsMapper.class);
		job.setReducerClass(CountRecsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}