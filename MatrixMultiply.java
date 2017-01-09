import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiply{
	private static int m = 4;
	private static int n = 3;
	private static int p = 2;
	public static class Map extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        	//get the current path of file
        	InputSplit inputsplit = context.getInputSplit();
        	String fileName = ((FileSplit) inputsplit).getPath().getName().toString();
        	System.out.println(fileName);
			String matrixName = new String();
			if(fileName.contains("a")){
				matrixName = "a";
			}
			else{
				matrixName = "b";
			}
			String line = value.toString();
			String[] nums = line.split(",");
			if(matrixName.compareTo("a") == 0){
				for(int i = 0;i<p;i++){
					String location = nums[0]+","+String.valueOf(i);
					String matrixNum = matrixName+","+nums[1]+","+nums[2];
					context.write(new Text(location),new Text(matrixNum));
				}
			}
			else{
				for(int i = 0;i<m;i++){
					String location = String.valueOf(i)+ ","+nums[1];
					String matrixNum = matrixName+","+nums[0]+","+nums[2];
					context.write(new Text(location),new Text(matrixNum));
				}

			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			int[] anums = new int[n];
			int[] bnums = new int[n];
			for(Text val : values){
				String numStr = val.toString();
				String[] nums = numStr.split(",");
				int j = Integer.parseInt(nums[1]);
				int num = Integer.parseInt(nums[2]);
				if(nums[0].compareTo("a") == 0){
					anums[j] = num;
				}
				else{
					bnums[j] = num;
				}
			}
			int sum = 0;
			for(int i = 0;i<anums.length;i++){
				sum += anums[i]*bnums[i];
			}
			context.write(key,new Text(String.valueOf(sum)));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: wordcount<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf,"MatrixMultiply");
		job.setJarByClass(MatrixMultiply.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		//job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}