package edu.stthomas.neuronhadoop;

/*
 * Mapper for Spiking Neuron Model.
 * It updates the intermediate state of each neuron and passes
 * the neuronal structure and synaptic weights of fired neurons
 * to the Reducer.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.MapFile;

import java.io.*;
import java.net.URI;
import java.util.*;

/*
 * Input key is the line number and value is the line content.
 * The mapper will output neuron id as key and text as value.
 * value can be either neuronal structure or synaptic weight.
 */
public class SpikingNeuronMapper 
extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Neuron neuron = new Neuron(); // Complex object represents neuronal structure.
	private Random randn = new Random();
	private String input_line; // original input
	private LongWritable neuron_id = new LongWritable();
	private Text neuron_string = new Text();
	//private SynapticWeightMatrix weight_matrix;
	private MapFile.Reader matrix_reader;
	private Text weightArray = new Text();
	
	// Counter for empty strings
	enum EmptyString {
		EMPTY_STRING
	}
	
	private double getGaussian() {
		return randn.nextGaussian();
	}

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("weight_matrix.m"), conf);
		matrix_reader = new MapFile.Reader(fs, "weight_matrix.m", conf);
	}

	private String[] getWeightsByID(LongWritable id) throws IOException {

		this.matrix_reader.get(neuron_id, this.weightArray);
		String[] weights = this.weightArray.toString().split(",");

		return weights;
	}

	@Override
	/*
	 * map method will be called for each input <key, value> pair.
	 */
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		input_line = value.toString();
		neuron.buildFromLine(input_line); // Build the neuronal structure.

		// Generate thalamic input.
		if (neuron.type.equals("e")) {
			neuron.current = 5 * this.getGaussian();
		} else if (neuron.type.equals("i")) {
			neuron.current = 2 * this.getGaussian();
		}

		// Update the current for the iteration
		neuron.current += neuron.synaptic_sum;
		// Update the membrane potential. Step 0.5 ms for numerical stability. 
		neuron.potential += 0.5 * (0.04*neuron.potential*neuron.potential + 5*neuron.potential
				+ 140 - neuron.recovery_var + neuron.current);
		neuron.potential += 0.5 * (0.04*neuron.potential*neuron.potential + 5*neuron.potential
				+ 140 - neuron.recovery_var + neuron.current);
		// Update membrane recovery variable.
		neuron.recovery_var += neuron.param_a * (neuron.param_b*neuron.potential - neuron.recovery_var);

		// Update number of iteration
		neuron.iter_num += 1;
		neuron.synaptic_sum = 0.0;
		neuron.fired = "N"; // Reset firing status

		// Construct the neuron ID for the MapFile lookup.
		neuron_id.set(neuron.id);

		// Check if the neuron has fired.
		if (neuron.potential >= 30.0) { // fired
			String[] weights = getWeightsByID(neuron_id);
			Text firing = new Text();
			// Emit firing information needed for the next iteration.
			for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
				//if (Math.abs(weights[i]) > 0.0) {
				firing.set(weights[i]);
				neuron_id.set(i+1); // ID starts from 1
				// Emit synaptic weight to neurons that connect with the fired neuron. 
				context.write(neuron_id, firing);
				//}
			}

			// Reset the membrane potential (voltage) and membrane recovery variable after firing.
			neuron.potential = neuron.param_c;
			neuron.recovery_var += neuron.param_d;
			neuron.fired = "Y"; // Update the firing status
		}

		// Construct the key
		neuron_id.set(neuron.id);
				
		// At last, emit the updated data structure of the neuron as the value.
		neuron_string.set(neuron.toLineFormat());
		context.write(neuron_id, neuron_string);
	}
}
