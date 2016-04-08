package supportpackage;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	private IntWritable[] values;
	
	public IntArrayWritable() {
		super((Class<? extends Writable>) IntWritable.class);
	}

	public IntArrayWritable(IntWritable[] arr) {
		super((Class<? extends Writable>) IntWritable.class);
		
		values = new IntWritable[arr.length];
		
		for(int i = 0; i < arr.length; i++) {
			values[i] = arr[i];
		}
		set(values);
	}

	@Override
	public String toString() {
		IntWritable[] values = (IntWritable[]) super.get();
		String ret = "";
		for(Writable str : values){
			ret = ret.concat(str.toString() + "\t");
		}
		return ret;
	}

}