package edu.stthomas.neuronhadoop;

/*
 * The Mapper for extracting fired neurons from the output.
 * It is executed after MapReduce job SpikingNeuronMode is done.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FiredNeuronsMapper 
extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

	private Neuron neuron = new Neuron(); // Complex object used to store data structure of a neuron.
	private LongWritable neuron_id = new LongWritable();
	private IntWritable time = new IntWritable();
	
	/*
	 * The Mapper emits number of iteration (how many milliseconds have gone)
	 * as the key and the neuron ID that has fired as the value to the Reducer.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		neuron.buildFromLine(line); // Get the neuron structure
		
		if (neuron.fired.equals("Y")) {
			neuron_id.set(neuron.id);
			time.set(neuron.iter_num);
			context.write(time, neuron_id);
		}
	}
}
