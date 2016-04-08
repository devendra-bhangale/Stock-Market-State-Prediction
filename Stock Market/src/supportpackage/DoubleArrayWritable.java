package supportpackage;

import java.text.DecimalFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {

	DecimalFormat two = new DecimalFormat();
	
	private DoubleWritable[] values;
	
	public DoubleArrayWritable() {
		super((Class<? extends Writable>) DoubleWritable.class);
	}

	public DoubleArrayWritable(DoubleWritable[] arr) {
		super((Class<? extends Writable>) DoubleWritable.class);

		two.setMaximumFractionDigits(8);
		two.setMinimumFractionDigits(8);
		
		values = new DoubleWritable[arr.length];
		
		for(int i = 0; i < arr.length; i++) {
			values[i] = new DoubleWritable(Double.parseDouble(two.format(arr[i].get())));
		}
		set(values);
	}

	@Override
	public String toString() {
		DoubleWritable[] values = (DoubleWritable[]) super.get();
		String ret = "";
		for(Writable str : values){
			ret = ret.concat(str.toString() + "\t");
		}
		return ret;
	}

}