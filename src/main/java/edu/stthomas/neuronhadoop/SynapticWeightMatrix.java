package edu.stthomas.neuronhadoop;

/*
 * Represents the synaptic weight matrix. The class reads the matrix from
 * a local file specified by distributed cache provided by Hadoop.
 */

import java.io.*;
import org.apache.hadoop.io.IOUtils;

public class SynapticWeightMatrix {
	public String[] synaptic_connection;

	public void init(File file) throws IOException {
		synaptic_connection = new String[Model.NUM_OF_NEURONS];	

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line;
			int line_counter = 0;
			while ((line = in.readLine()) != null) {
				synaptic_connection[line_counter++] = line;
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	public double[] getWeightsByID(long id) {
		double[] weights = new double[Model.NUM_OF_NEURONS];
		
		String[] connections = this.synaptic_connection[(int)id].split(",");
		for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
			weights[i] = Double.parseDouble(connections[i]);
		}
		
		return weights;
	}
}
