package com.arun.top;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Driver {
public static void main(String[] args1) throws IOException, ClassNotFoundException, InterruptedException {

	Configuration conf =  new Configuration();

	String args[] =new GenericOptionsParser(conf,args1).getRemainingArgs();
	
	if (args.length != 4) {
		System.err.println("Please specify the input and output path");
		System.exit(-1);
		}

	
	
	
	Job sampleJob = Job.getInstance(conf);
	sampleJob.setJarByClass(Driver.class);
	sampleJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
	TextOutputFormat.setOutputPath(sampleJob, new Path(args[2]));
	sampleJob.setOutputKeyClass(Text.class);
	sampleJob.setOutputValueClass(Text.class);
	sampleJob.setReducerClass(JoinReducer.class);
	MultipleInputs.addInputPath(sampleJob, new Path(args[0]), TextInputFormat.class, MoviesMapper.class);
	MultipleInputs.addInputPath(sampleJob, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);
	int code = sampleJob.waitForCompletion(true) ? 0 : 1;
	if (code == 0) {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		job.setJobName("Highest_Rated_Movies");
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(HighestMapper.class);
		job.setReducerClass(HighestReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}
}