package edu.stthomas.neuronhadoop;

/*
 * Represents the synaptic weight matrix. The class reads the matrix from
 * a local file specified by distributed cache provided by Hadoop.
 */

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

public class SynapticWeightMatrix {
	public List<String> synaptic_connection;

	public void init(File file) throws IOException {
		
		synaptic_connection = new ArrayList<String>();
				
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line;
			while ((line = in.readLine()) != null) {
				synaptic_connection.add(line);
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	public ArrayList<Double> getWeightsByID(long id) {
		ArrayList<Double> weights = new ArrayList<Double>();
		
		System.out.println("Size of the array list: " + this.synaptic_connection.size());
		String[] connections = this.synaptic_connection.get((int)id-1).split(",");
		for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
			weights.add(Double.parseDouble(connections[i]));
		}
		
		return weights;
	}
}
