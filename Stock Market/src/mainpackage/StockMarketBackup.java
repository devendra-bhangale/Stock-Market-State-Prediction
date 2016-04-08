package mainpackage;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

import supportpackage.IntArrWritable;

public class StockMarketBackup {

	private static final int WEEKDAYS = 5;
	
	private static int tokenLength = 0;
	
	// Mapper class
	public static class avgMapper extends MapReduceBase
				implements	Mapper<LongWritable,	/* Input key Type */
							Text,					/* Input value Type */
							Text,					/* Output key Type */
							IntArrWritable>			/* Output value Type */
	{

		private static int mapCounter = 0;
		
		// Map function
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrWritable> output, Reporter reporter)
				throws IOException {

			mapCounter++;
			System.out.println("mapper: " + mapCounter);

			String				line	= value.toString();
			StringTokenizer		tokens	= new StringTokenizer(line, "\t");
			String				company	= tokens.nextToken();

			tokenLength = tokens.countTokens();
			IntWritable prices[]	= new IntWritable[WEEKDAYS];

			int index = 0;
			while (tokens.hasMoreTokens()) {
				prices[index]		= new IntWritable(Integer.parseInt(tokens.nextToken()));
				index++;
				
				if(index == WEEKDAYS){
					output.collect(new Text(company), new IntArrWritable(prices));
					index = 0;
				}
			}
		}
	}

	// Reducer class
	public static class avgReduce extends MapReduceBase implements Reducer<Text, IntArrWritable, Text, IntArrWritable> {

		private static int reducerCounter = 0, newIndex = 0;

		// Reduce function
		public void reduce(Text key, Iterator<IntArrWritable> values, OutputCollector<Text, IntArrWritable> output,
				Reporter reporter) throws IOException {

			reducerCounter++;	System.out.println("reducer: " + reducerCounter);

			IntWritable	prices[]	= new IntWritable[WEEKDAYS];
			IntWritable	avgPrices[]	= new IntWritable[tokenLength / WEEKDAYS];	//[1];
			IntWritable	sum			= new IntWritable(0);

			int index = 0, count = 0;
			while (values.hasNext()) {	
				index = 0;	count = 0;
				ArrayWritable val = values.next();
				for(Writable writable: val.get()){
					prices[index] = (IntWritable)writable;
					index++;
				}

				index = 0;	count = 0;
				index = (newIndex == 1) ? 1 : 0;
				for (int i = 0; i < prices.length; i++) {
					sum = new IntWritable(sum.get() + prices[i].get());
					count++;

					if (count == WEEKDAYS) {
						newIndex = (index == 0) ? 1 : 0;
						avgPrices[index] = new IntWritable(sum.get() / WEEKDAYS);
						index++;
						sum = new IntWritable(0);
						count = 0;
					}

				}
				//output.collect(key, new IntArrWritable(avgPrices));
			}output.collect(key, new IntArrWritable(avgPrices));
		}
	}

	// Main function
	public static void main(String args[]) throws Exception {

		JobConf conf = new JobConf(StockMarket.class);

		conf.setJobName("Average_Closing_Prices");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntArrWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntArrWritable.class);

		conf.setMapperClass(avgMapper.class);
		//conf.setCombinerClass(avgReduce.class);
		conf.setReducerClass(avgReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
		
}