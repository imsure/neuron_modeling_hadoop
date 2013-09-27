package edu.stthomas.neuronhadoop;

/*
 * Reducer for Neuron Data Extraction job.
 * It collects neuron IDs that fired at a specific time
 * and write <time, ID> key value pair to the output.
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FiredNeuronsReducer 
extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		for (LongWritable value : values) {
			context.write(key, value);
		}
	}
}
