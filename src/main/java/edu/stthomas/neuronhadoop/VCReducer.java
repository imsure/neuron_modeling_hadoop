package edu.stthomas.neuronhadoop;

/*
 * Reducer for  Membrane Potential Evolution job.
 * It emits <neuron id, time, membrane potential> as output.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class VCReducer 
extends Reducer<LongWritable, Text, LongWritable, Text> {

	private HashMap<Integer, String> time2voltage = new HashMap<Integer, String>();
	private Text output = new Text();
	SortedSet<Integer> times = new TreeSet<Integer>();
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		for (Text value : values) {
			// Split the value to time 't' and voltage 'v'.
			String[] vals = value.toString().split("_");
			int t = Integer.parseInt(vals[0]);
			String v = vals[1];
			time2voltage.put(t, v);
		}

		//SortedSet<Integer> keys = new TreeSet<Integer>(time2voltage.keySet());
		times.clear();
		times.addAll(time2voltage.keySet());
		for (int time : times) {
			// The output would be neuron id	time	voltage
			output.set(Integer.toString(time) + "\t" + time2voltage.get(time));
			context.write(key, output);
		}
	}
}
