package mainpackage;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import supportpackage.DoubleArrayWritable;
import supportpackage.IntArrayWritable;

public class StockMarket {

	/*************************************************************************************************/

	/*************************************************************************************************/
	/*************************************************************************************************/
	private static final int WEEKDAYS = 5;
	private static int avgTokensLength = 0;

	// Job1: Average Mapper class
	public static class avgMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

		// Average Map function
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleArrayWritable> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line, "\t");
			String company = tokens.nextToken();

			avgTokensLength = tokens.countTokens();
			DoubleWritable prices[] = new DoubleWritable[WEEKDAYS];

			int index = 0;
			while (tokens.hasMoreTokens()) {
				prices[index] = new DoubleWritable(Double.parseDouble(tokens.nextToken()));
				index++;

				if (index == WEEKDAYS) {
					output.collect(new Text(company), new DoubleArrayWritable(prices));
					index = 0;
				}
			}
		}
	}

	/*************************************************************************************************/
	// Job1: Average Reducer class
	public static class avgReduce extends MapReduceBase
			implements Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {

		private static int newIndex = 0;

		// Average Reduce function
		public void reduce(Text key, Iterator<DoubleArrayWritable> values,
				OutputCollector<Text, DoubleArrayWritable> output, Reporter reporter) throws IOException {

			DoubleWritable prices[] = new DoubleWritable[WEEKDAYS];
			DoubleWritable avgPrices[] = new DoubleWritable[avgTokensLength / WEEKDAYS];
			DoubleWritable sum = new DoubleWritable(0);

			int index = 0, count = 0;
			while (values.hasNext()) {
				index = 0;
				count = 0;
				ArrayWritable val = values.next();
				for (Writable writable : val.get()) {
					prices[index] = (DoubleWritable) writable;
					index++;
				}

				// index = 0;
				count = 0;
				for (int i = 0; i < prices.length; i++) {
					sum = new DoubleWritable(sum.get() + prices[i].get());
					count++;

					if (count == WEEKDAYS) {
						avgPrices[newIndex] = new DoubleWritable(sum.get() / WEEKDAYS);
						newIndex++;
						sum = new DoubleWritable(0);
						count = 0;
					}
				}
			}
			output.collect(key, new DoubleArrayWritable(avgPrices));
			newIndex = 0;
		}
	}

	/*************************************************************************************************/

	/*************************************************************************************************/
	/*************************************************************************************************/
	private static int transTokensLength = 0;
	private static double bl_bl = 0, bl_br = 0, bl_st = 0;
	private static double br_bl = 0, br_br = 0, br_st = 0;
	private static double st_bl = 0, st_br = 0, st_st = 0;

	// Job2: Transition Mapper class
	public static class transMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

		// Transition Map function
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleArrayWritable> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line, "\t");
			String company = tokens.nextToken();

			transTokensLength = tokens.countTokens();
			DoubleWritable averages[] = new DoubleWritable[transTokensLength];

			int index = 0;
			while (tokens.hasMoreTokens()) {
				averages[index] = new DoubleWritable(Double.parseDouble(tokens.nextToken()));
				index++;
			}
			DoubleWritable prev = averages[0], curr = averages[1];

			int comp = 0, lastcomp = 0;
			double th = 0.025; // th: threshold
			comp = (curr.get() > (prev.get() + th)) ? 1 : comp;
			comp = (curr.get() < (prev.get() - th)) ? -1 : comp;

			for (index = 2; index < averages.length; index++) {
				lastcomp = comp;
				prev = curr;
				curr = averages[index];

				comp = 0; // 0 = stag
				comp = (curr.get() > (prev.get() + th)) ? 1 : comp; // 1 = bull
				comp = (curr.get() < (prev.get() - th)) ? -1 : comp; // -1 =
																		// bear

				switch (comp) {

				case 1: // earlier it was bull
					bl_bl += (lastcomp == 1) ? 1 : 0;
					bl_st += (lastcomp == 0) ? 1 : 0;
					bl_br += (lastcomp == -1) ? 1 : 0;
					break;
				case 0: // earlier it was stag
					st_bl += (lastcomp == 1) ? 1 : 0;
					st_st += (lastcomp == 0) ? 1 : 0;
					st_br += (lastcomp == -1) ? 1 : 0;
					break;
				case -1: // earlier it was bear
					br_bl += (lastcomp == 1) ? 1 : 0;
					br_st += (lastcomp == 0) ? 1 : 0;
					br_br += (lastcomp == -1) ? 1 : 0;
					break;
				}
			}
			DoubleWritable[] bull = new DoubleWritable[3];
			bull[0] = new DoubleWritable(bl_bl / 50);
			bull[1] = new DoubleWritable(bl_st / 50);
			bull[2] = new DoubleWritable(bl_br / 50);
			DoubleWritable[] stag = new DoubleWritable[3];
			stag[0] = new DoubleWritable(st_bl / 50);
			stag[1] = new DoubleWritable(st_st / 50);
			stag[2] = new DoubleWritable(st_br / 50);
			DoubleWritable[] bear = new DoubleWritable[3];
			bear[0] = new DoubleWritable(br_bl / 50);
			bear[1] = new DoubleWritable(br_st / 50);
			bear[2] = new DoubleWritable(br_br / 50);

			output.collect(new Text(company), new DoubleArrayWritable(bull));
			output.collect(new Text(company), new DoubleArrayWritable(stag));
			output.collect(new Text(company), new DoubleArrayWritable(bear));

			bl_bl = 0;
			bl_br = 0;
			bl_st = 0;
			br_bl = 0;
			br_br = 0;
			br_st = 0;
			st_bl = 0;
			st_br = 0;
			st_st = 0;

		}

	}

	/*************************************************************************************************/
	// Job2: Transition Reducer class
	public static class transReduce extends MapReduceBase
			implements Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {

		// Transition Reduce function
		public void reduce(Text key, Iterator<DoubleArrayWritable> values,
				OutputCollector<Text, DoubleArrayWritable> output, Reporter reporter) throws IOException {

			DoubleWritable transitions[] = new DoubleWritable[9]; // 9: elements
																	// for 3*3
																	// matrix

			int index = 0;
			while (values.hasNext()) {
				ArrayWritable val = values.next();
				for (Writable writable : val.get()) {
					transitions[index] = (DoubleWritable) writable;
					index++;
				}
			}
			output.collect(key, new DoubleArrayWritable(transitions));
		}
	}

	/*************************************************************************************************/

	/*************************************************************************************************/
	/*************************************************************************************************/
	private static final int M = 3;
	private static final int N = 3;

	// Job3: Convergence Mapper class
	public static class convMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

		// Convergence Map function
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleArrayWritable> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line, "\t");
			String company = tokens.nextToken();

			DoubleWritable[] matrix = new DoubleWritable[9];

			int index = 0;
			while (tokens.hasMoreTokens()) {
				matrix[index] = new DoubleWritable(Double.parseDouble(tokens.nextToken()));
				index++;
			}

			output.collect(new Text(company), new DoubleArrayWritable(matrix));
		}
	}

	/*************************************************************************************************/
	// Job3: Convergence Reducer class
	public static class convReduce extends MapReduceBase
			implements Reducer<Text, DoubleArrayWritable, Text, IntWritable> {

		// Convergence Reduce function
		public void reduce(Text key, Iterator<DoubleArrayWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			DoubleWritable[][] matrix = new DoubleWritable[M][N];
			DoubleWritable[][] oldProduct = new DoubleWritable[M][N];
			DoubleWritable[][] newProduct = new DoubleWritable[M][N];

			int m = 0, n = 0;
			while (values.hasNext()) {
				ArrayWritable val = values.next();
				for (Writable writable : val.get()) {
					matrix[m][n] = (DoubleWritable) writable;
					oldProduct[m][n] = (DoubleWritable) writable;
					n++;
					m += (n > (N - 1)) ? 1 : 0;
					n = (n > (N - 1)) ? 0 : n;
				}
				m++;
				n = 0;
			}

			int limit_N = 0;
			boolean limit = false;
			double sum = 0;
			while (!limit) {
				limit_N++;
				
				for (int i = 0; i < M; i++) { // Matrix Multiplication
					for (int j = 0; j < N; j++) {
						for (int k = 0; k < N; k++) {
							sum += oldProduct[i][k].get() * matrix[k][j].get();
						}
						newProduct[i][j] = new DoubleWritable(sum);
						sum = 0;
					}
				}

				int match = 0;
				double th = 0.001; // threshold
				for (int i = 0; i < M; i++) { // Matrix Comparison
					for (int j = 0; j < N; j++) {
						double old, nw, sub;
						old = oldProduct[i][j].get();
						nw = newProduct[i][j].get();
						sub = Math.abs((old - nw));
						if (sub < th)
							match++;
					}
				}

				if (match == (M*N)) {
					output.collect(new Text(key), new IntWritable(limit_N));
					limit = true;
				} else {
					match = 0;
					for (int i = 0; i < 3; i++) {
						for (int j = 0; j < 3; j++) {
							oldProduct[i][j] = newProduct[i][j];
						}
					}
				}

			}
		}
	}

	/*************************************************************************************************/

	/*************************************************************************************************/
	/*************************************************************************************************/
	// Main function
	public static void main(String args[]) throws Exception {

		// Job1: finding averages
		JobConf average = new JobConf(StockMarket.class);

		average.setJobName("Averages");

		average.setMapOutputKeyClass(Text.class);
		average.setMapOutputValueClass(DoubleArrayWritable.class);
		average.setOutputKeyClass(Text.class);
		average.setOutputValueClass(DoubleArrayWritable.class);

		average.setMapperClass(avgMapper.class);
		// average.setCombinerClass(avgReduce.class);
		average.setReducerClass(avgReduce.class);

		average.setInputFormat(TextInputFormat.class);
		average.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(average, new Path(args[0]));
		FileOutputFormat.setOutputPath(average, new Path(args[1]));

		JobClient.runJob(average);

		/*************************************************************************************************/
		// Job2: check bear, bull, stagnant transitions
		JobConf transition = new JobConf(StockMarket.class);

		transition.setJobName("Transition");

		transition.setMapOutputKeyClass(Text.class);
		transition.setMapOutputValueClass(DoubleArrayWritable.class);
		transition.setOutputKeyClass(Text.class);
		transition.setOutputValueClass(DoubleArrayWritable.class);

		transition.setMapperClass(transMapper.class);
		// transition.setCombinerClass(transReduce.class);
		transition.setReducerClass(transReduce.class);

		transition.setInputFormat(TextInputFormat.class);
		transition.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(transition, new Path(args[2]));
		FileOutputFormat.setOutputPath(transition, new Path(args[3]));

		JobClient.runJob(transition);

		/*************************************************************************************************/
		// Job3: convergence limit
		JobConf convergence = new JobConf(StockMarket.class);

		convergence.setJobName("Convergence");

		convergence.setMapOutputKeyClass(Text.class);
		convergence.setMapOutputValueClass(DoubleArrayWritable.class);
		convergence.setOutputKeyClass(Text.class);
		convergence.setOutputValueClass(IntWritable.class);

		convergence.setMapperClass(convMapper.class);
		// convergence.setCombinerClass(transReduce.class);
		convergence.setReducerClass(convReduce.class);

		convergence.setInputFormat(TextInputFormat.class);
		convergence.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(convergence, new Path(args[4]));
		FileOutputFormat.setOutputPath(convergence, new Path(args[5]));

		JobClient.runJob(convergence);
	}

}

/*
argument paths:

/home/devendra/stockTest/Data.txt /home/devendra/stockTest/output/avg_output /home/devendra/stockTest/output/avg_output/part-00000 /home/devendra/stockTest/output/trans_output /home/devendra/stockTest/output/trans_output/part-00000 /home/devendra/stockTest/output/convergence_output

*/