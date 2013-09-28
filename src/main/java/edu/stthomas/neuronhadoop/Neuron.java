package edu.stthomas.neuronhadoop;

/*
 * A complex structure represents a neuron.
 */

public class Neuron {

	/*
	 * Fields hold neuronal data.
	 */
	public long id = 0; // Neuron ID
	public String type; // Type of Neuron
	public int iter_num = 0;
	public double param_a = 0.0;
	public double param_b = 0.0;
	public double param_c = 0.0;
	public double param_d = 0.0;
	public double recovery_var = 0.0;
	public double potential = 0.0;
	public double synaptic_sum = 0.0;
	public double[] synaptic_connection;
	public String fired; // A neuron fired or not.
	public double current = 0.0; // Thalamic input current

	public Neuron() {
		synaptic_connection = new double[Model.NUM_OF_NEURONS];		
	}

	/*
	 * Build a neuron structure from 'line'.
	 */
	public void buildFromLine(String line) {
		
		String[] elems = line.split(";");

		/*
		 * Convert and assign corresponding values to the fields.
		 */
		id = Integer.parseInt(elems[0]);
		type = elems[1];
		iter_num = Integer.parseInt(elems[2]);
		param_a = Double.parseDouble(elems[3]);
		param_b = Double.parseDouble(elems[4]);
		param_c = Double.parseDouble(elems[5]);
		param_d = Double.parseDouble(elems[6]);
		recovery_var = Double.parseDouble(elems[7]);
		potential = Double.parseDouble(elems[8]);
		synaptic_sum = Double.parseDouble(elems[9]);

		String[] connections = elems[10].split(",");
		for (int i = 0; i < Model.NUM_OF_NEURONS; i++) {
			synaptic_connection[i] = Double.parseDouble(connections[i]);
		}

		fired = elems[11];
	}
	
	/*
	 * Convert the data fields back to a line of string.
	 */
	public String toLineFormat() {
		String update = new String();

		update += Long.toString(this.id) + ";";
		update += this.type + ";";
		update += Integer.toString(this.iter_num) + ";";
		update += Double.toString(this.param_a) + ";";
		update += Double.toString(this.param_b) + ";";
		update += Double.toString(this.param_c) + ";";
		update += Double.toString(this.param_d) + ";";
		update += Double.toString(this.recovery_var) + ";";
		update += Double.toString(this.potential) + ";";
		update += Double.toString(this.synaptic_sum) + ";";

		for (int i = 0; i < (Model.NUM_OF_NEURONS-1); i++) {
			update += Double.toString(this.synaptic_connection[i]) + ",";
		}
		// Don't forget the last one without a trailing comma.
		update += Double.toString(this.synaptic_connection[Model.NUM_OF_NEURONS-1]);

		update += ";" + this.fired;

		return update;
	}
}
