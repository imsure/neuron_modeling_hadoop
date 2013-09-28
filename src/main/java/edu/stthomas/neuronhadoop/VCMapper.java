/*
 * The Mapper for Membrane Potential Evolution job.
 * It is executed after Spiking Neuron job is done.
 *  
 */
package edu.stthomas.neuronhadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VCMapper
extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Neuron neuron = new Neuron(); // Complex object used to store data structure of a neuron.
	private LongWritable neuron_id = new LongWritable();
	private String time_voltage = new String();
	private Text emit_value = new Text();
	
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
		time_voltage = Integer.toString(neuron.iter_num);
		time_voltage +=  "_"; // Don't use hyphen because potential could be negative.
		time_voltage += Double.toString(neuron.potential);

		neuron_id.set(neuron.id);
		emit_value.set(time_voltage);
		context.write(neuron_id, emit_value);
	}
}
