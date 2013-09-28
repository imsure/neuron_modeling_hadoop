package edu.stthomas.neuronhadoop;

/*
 * Reducer for Spiking Neuron Model.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SynapticWeightsReducer 
extends Reducer<LongWritable, Text, NullWritable, Text> {

	private Neuron neuron = new Neuron(); // Complex object used to store recoveried neuron data structure. 
	private Text neuron_string = new Text();
	
	/*
	 * Decide if a string 'line' represents a neuron data structure
	 * or just a synaptic connection weight (in this case, 
	 * there will no delimiter ';' found, it is just a double).
	 */
	private boolean isNeuron(String line) {
		String delimiter = ";";
		int num_delimiter = line.split(delimiter).length - 1;

		if (num_delimiter > 0) {
			return true; // is a neuron
		} else {
			return false; // synaptic connection weight
		}
	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		double weight_sum = 0.0;
		double weight;
		String line;
		for (Text value : values) {
			line = value.toString();
			if (isNeuron(line) == true) {
				neuron.buildFromLine(line); // Get the complex object of neuron.
			} else { // Adds up synaptic connection weight from neurons that have fired.
				weight = Double.parseDouble(line);
				weight_sum += weight;
			}
		}

		// Update synaptic connection weights summation.
		neuron.synaptic_sum = weight_sum;

		//Convert the neuron data structure to a line of string.
		String update = neuron.toLineFormat();

		// Emit, key is null because neuron id is aleardy inside the value.
		neuron_string.set(update);
		context.write(NullWritable.get(), neuron_string);
	}
}
