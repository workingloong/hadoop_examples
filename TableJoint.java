import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TableJoint{
	public static int mapNum = 0;
	public static class Map extends Mapper<Object, Text,Text,Text>{
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName().toString();
			String line = value.toString();
			String[] names = line.split(" ");
			String childname = new String();
			String parentname = new String();
			if(names[0].compareTo("child") != 0){
				childname = names[0];
				parentname = names[1];
				context.write(new Text(names[1]),new Text("1"+childname+" "+parentname));
				context.write(new Text(names[0]),new Text("2"+childname+" "+parentname));

			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		public static boolean firstline = true;
		public void reduce (Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
			List<String> grandchilds = new ArrayList<>();
			List<String> grandparents = new ArrayList<>();
			for(Text val : values){
				String name = val.toString();
				char relationType = name.charAt(0);
				String subname = name.substring(1);
				String[] names = subname.split(" ");
				if(relationType == '1') grandchilds.add(names[0]);
				else grandparents.add(names[1]);
			}
			for(int i = 0;i<grandchilds.size();i++){
				for(int j =0 ;j<grandparents.size();j++){
					if(firstline){
						context.write(new Text("grandchild"), new Text("grandparent"));
						firstline = false;
					}
					else context.write(new Text(grandchilds.get(i)), new Text(grandparents.get(j)));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: wordcount<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf,"TableJoint");
		job.setJarByClass(TableJoint.class);
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