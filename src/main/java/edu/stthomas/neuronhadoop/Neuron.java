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
	public String fired; // A neuron fired or not.
	public double current = 0.0; // Thalamic input current

	public StringBuilder sb = new StringBuilder(60); // used for string concatenation to improve performance.
													 // 60 is pre-calculated based on the size of input record.
	
	public Neuron() {
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

		fired = elems[10];
	}
	
	/*
	 * Convert the data fields back to a line of string.
	 */
	public String toLineFormat() {
		
		sb.setLength(0); // sb is used for many times, so we need to make sure it is reset before using.
		sb.append(this.id).append(';'); // Note: using a single char is more efficient than ";"
		sb.append(this.type).append(';');
		sb.append(this.iter_num).append(';');
		sb.append(this.param_a).append(';');
		sb.append(this.param_b).append(';');
		sb.append(this.param_c).append(';');
		sb.append(this.param_d).append(';');
		sb.append(this.recovery_var).append(';');
		sb.append(this.potential).append(';');
		sb.append(this.synaptic_sum).append(';');
		sb.append(this.fired);

		return sb.toString();
	}
}
