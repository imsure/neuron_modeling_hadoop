package edu.stthomas.neuronhadoop;
/*
 * Hadoop application for Spiking Neuron Model which is based on Izhikevich's model.
 */ 

import org.apache.hadoop.fs.Path;
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
	public final static int TIME_IN_MS = 100;

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Model <input path> <output path>");
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
		return (success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Model(), args);
		System.exit(res);
	}
}

