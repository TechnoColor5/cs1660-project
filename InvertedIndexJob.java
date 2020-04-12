import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.*;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;

public class InvertedIndexJob {
	public static void main(String args[]) 
		throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job();
		job.setJarByClass(InvertedIndexJob.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

	static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();

		public InvertedIndexMapper() {}

		public void map(LongWritable key, Text value, Context context)
				throws IOException,InterruptedException
		{
			/*Get the name of the file using context.getInputSplit()method*/
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String line = value.toString();

			//Split the line in words
			String words[]=line.split(" ");
			StringTokenizer tokenizer = new StringTokenizer(line);

			while(tokenizer.hasMoreTokens()){
				//Get rid of punctuation
				word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());

				//Write out <Term, Filename> pairs
				context.write(word, new Text(fileName));
			}
			}
		}

	 static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		public InvertedIndexReducer() {}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//Holds <term/filename, count> pairs
			HashMap m=new HashMap();

			int count=0;
			for(Text t:values){
				String str=t.toString();

				if(m!=null && m.get(str)!=null){
					count=(int)m.get(str);
					m.put(str, ++count);
				}else{
					m.put(str, 1);
				}
			}
			//Writes the HashMap
			context.write(key, new Text(m.toString()));
		}
	}
}
