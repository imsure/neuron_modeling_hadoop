package edu.stthomas.neuronhadoop;
/*
 * Hadoop application for Spiking Neuron Model which is based on Izhikevich's model.
 */ 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

public class Model extends Configured implements Tool {

	private String IN;
	private String OUT;
	public final static int NUM_OF_NEURONS = 500;
	public final static int TIME_IN_MS = 200;

	public int run(String[] args) throws Exception {

		if (args.length != 5) {
			System.err.println("Usage: Model <input path> " +
					"<output path for neuron structure>" +
					"<output path for firing extraction>" +
					"<output path for membrane potential>" +
					"<output path for recovery variable>");
			System.exit(-1);
		}

		IN = args[0];
		OUT = args[1];

		int timer = 1;
		boolean success = false;

		String inpath = IN;
		// Each iteration we need a different output path because
		// we are going to chain map-reduce together. The current output
		// of the reducer will be the input of the next stage's mapper's input.
		String outpath = OUT + timer;

		while (timer <= TIME_IN_MS) {	

			Job job = new Job(getConf());
			job.setJarByClass(Model.class);
			job.setJobName("Izhikevich Model");

			FileInputFormat.addInputPath(job, new Path(inpath));
			FileOutputFormat.setOutputPath(job, new Path(outpath));

			job.setMapperClass(SpikingNeuronMapper.class);
			job.setReducerClass(SynapticWeightsReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			success = job.waitForCompletion(true);
			if (success == false) {
				break;
			}

			// Reset the input and output path for the next iteration
			inpath = outpath;
			timer++;
			outpath = OUT + timer;
		}
		
		/*
		 * Start jobs for post-analysis
		 */
		String inpaths = new String(); // used to construct a comma separated input paths.
		
		for (int i = 1; i < TIME_IN_MS; i++) {
		    inpaths += "out-dir" + i + ",";
		}
		inpaths += "out-dir" + TIME_IN_MS;
		
		if (success == true) {				
			Job job = new Job(getConf());
			job.setJarByClass(Model.class);
			job.setJobName("Fired Neurons Extraction");

			FileInputFormat.addInputPaths(job, inpaths);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setMapperClass(FiredNeuronsMapper.class);
			job.setReducerClass(FiredNeuronsReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.setNumReduceTasks(1);
			
			success = job.waitForCompletion(true);
		}
		
		if (success == true) {				
			Job job = new Job(getConf());
			job.setJarByClass(Model.class);
			job.setJobName("Membrane Potentials Extraction");

			FileInputFormat.addInputPaths(job, inpaths);
			FileOutputFormat.setOutputPath(job, new Path(args[3]));

			job.setMapperClass(VCMapper.class);
			job.setReducerClass(VCReducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);
			
			success = job.waitForCompletion(true);
		}
		
		if (success == true) {				
			Job job = new Job(getConf());
			job.setJarByClass(Model.class);
			job.setJobName("Recovery Variable Extraction");

			FileInputFormat.addInputPaths(job, inpaths);
			FileOutputFormat.setOutputPath(job, new Path(args[4]));

			job.setMapperClass(RVMapper.class);
			job.setReducerClass(RVReducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);
			
			success = job.waitForCompletion(true);
		}
		
		return (success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Model(), args);
		System.exit(res);
	}
}

