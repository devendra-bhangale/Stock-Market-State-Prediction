package supportpackage;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrWritable extends ArrayWritable {

	private IntWritable[] values;
	
	public IntArrWritable() {
		super((Class<? extends Writable>) IntWritable.class);
	}

	public IntArrWritable(IntWritable[] arr) {
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
		for(Writable str : values)
			ret = ret.concat(str.toString() + "\t");
		return ret;
	}

}
