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
	private String update; // hold updated neuronal information.
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

	private ArrayList<Double> getWeightsByID(LongWritable id, Context context) throws IOException {
		ArrayList<Double> weights = new ArrayList<Double>();

		this.matrix_reader.get(neuron_id, this.weightArray);
		String[] connections = this.weightArray.toString().split(",");
		for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
			if (connections[i].isEmpty()) {
				context.setStatus("Empty string found!!! Reasons not clear.");
				context.getCounter(EmptyString.EMPTY_STRING).increment(1);
				System.out.println("Index of weight: " + i);
				System.out.println("Neuron ID: " + id.get());
				//continue;
			}
			weights.add(Double.parseDouble(connections[i]));
		}

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
			ArrayList<Double> weights = getWeightsByID(neuron_id, context);
			Text firing = new Text();
			// Emit firing information needed for the next iteration.
			for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
				if (Math.abs(weights.get(i)) > 0.0) {
					firing.set(Double.toString(weights.get(i)));
					neuron_id.set(i+1); // ID starts from 1
					// Emit synaptic weight to neurons that connect with the fired neuron. 
					context.write(neuron_id, firing);
				}
			}

			// Reset the membrane potential (voltage) and membrane recovery variable after firing.
			neuron.potential = neuron.param_c;
			neuron.recovery_var += neuron.param_d;
			neuron.fired = "Y"; // Update the firing status
		}

		// Convert to line format.
		update = neuron.toLineFormat();

		// Construct the key
		neuron_id.set(neuron.id);
				
		// At last, emit the updated data structure of the neuron as the value.
		neuron_string.set(update);
		context.write(neuron_id, neuron_string);
	}
}
