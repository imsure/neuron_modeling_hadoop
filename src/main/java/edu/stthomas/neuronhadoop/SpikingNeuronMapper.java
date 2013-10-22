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
	private Map<Integer, Double> weight_map; // For in-mapper combiner

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
		/*
		 * Set up MapFile for synaptic weight matrix lookup.
		 */
		Configuration conf = new Configuration();
		String mapfile = "weight_matrix_100000.m";
		FileSystem fs = FileSystem.get(URI.create(mapfile), conf);
		matrix_reader = new MapFile.Reader(fs, mapfile, conf);

		// Set up Hash Map for in mapper combiner.
		this.weight_map = new HashMap<Integer, Double>();
	}

	private ArrayList<Double> getWeightsByID(LongWritable id) throws IOException {
		ArrayList<Double> weights = new ArrayList<Double>();

		this.weightArray.clear();
		this.matrix_reader.get(neuron_id, this.weightArray);
		String[] connections = this.weightArray.toString().split(",");
		for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
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
		} else {
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
			ArrayList<Double> weights = getWeightsByID(neuron_id);

			// In Mapper combiner
			for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
				Double accumulated_weight = this.weight_map.get(i+1);
				double current_weight = weights.get(i);
				if (accumulated_weight == null) {
					this.weight_map.put(i+1, current_weight);
				} else {
					this.weight_map.put(i+1, accumulated_weight+current_weight);
				}

			}

			// Reset the membrane potential (voltage) and membrane recovery variable after firing.
			neuron.potential = neuron.param_c;
			neuron.recovery_var += neuron.param_d;
			neuron.fired = "Y"; // Update the firing status
		}

		// Construct the key
		//neuron_id.set(neuron.id);

		// At last, emit the updated data structure of the neuron as the value.
		neuron_string.set(neuron.toLineFormat());
		context.write(neuron_id, neuron_string);
	}

	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		Text firing = new Text();
		LongWritable neuron_id = new LongWritable();

		for (Map.Entry<Integer, Double> entry : this.weight_map.entrySet()) {
			neuron_id.set(entry.getKey());
			firing.set(Double.toString(entry.getValue()));
			// Emit 
			context.write(neuron_id, firing);
		}
	}

}
