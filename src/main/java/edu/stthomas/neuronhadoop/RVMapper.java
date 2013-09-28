package edu.stthomas.neuronhadoop;

/*
 * The Mapper for Recovery Variable Evolution job.
 * It is executed after Spiking Neuron job is done.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RVMapper 
extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Neuron neuron = new Neuron(); // Complex object used to store data structure of a neuron.
	private LongWritable neuron_id = new LongWritable();
	private Text output = new Text();

	/*
	 * The Mapper need to emit the neuron ID as the key
	 * and the string: time-voltage(membrane potential) as the value to the Reducer.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();

		// Parse and get the neuron data.
		neuron.buildFromLine(line);
		String time_recovery = Integer.toString(neuron.iter_num);
		time_recovery +=  "_"; // Don't use hyphen because potential could be negative.
		time_recovery += Double.toString(neuron.recovery_var);

		neuron_id.set(neuron.id);
		output.set(time_recovery);
		context.write(neuron_id, output);
	}
}
