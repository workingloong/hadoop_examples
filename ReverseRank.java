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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReverseRank{
	public static class MyMap extends Mapper<Object,Text,Text,Text>{
		public static int docID = 0;
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			String line = value.toString();
			int position = 0;
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
			 	String word = tokenizer.nextToken();
			 	if(word.charAt(word.length()-1) >'z' || word.charAt(word.length()-1) <'a'){
			 		word = word.substring(0,word.length()-1);
			 	}
			 	word.toLowerCase();
			 	String info = "doc"+String.valueOf(docID)+","+ String.valueOf(position);
			 	position++;
			 	context.write(new Text(word), new Text(info));
			}
			docID++;
		}
	}
	public static class MyReduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			Map<String,List<Integer>> map = new HashMap<>();
			for(Text val : values){
				String info = val.toString();
				String[] docAndPos = info.split(",");
				if(!map.containsKey(docAndPos[0])){
					List<Integer> tempList = new ArrayList<>();
					map.put(docAndPos[0],tempList);
				}
				int position = Integer.parseInt(docAndPos[1]);
				map.get(docAndPos[0]).add(position);
			}
			Iterator iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry entry = (Map.Entry)iter.next();
				String doc = (String)entry.getKey();
				List<Integer> list = (List<Integer>)entry.getValue();
				Collections.sort(list);
				String info = list.toString();
				String record = doc+":"+info;
				context.write(key,new Text(record));
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   		if (otherArgs.length != 2) {
     		System.err.println("Usage: wordcount <in> <out>");
     		System.exit(2);
   		}
   		Job job = new Job(conf,"RevereseRank");
   		job.setJarByClass(ReverseRank.class);
   		job.setMapperClass(MyMap.class);
   		job.setInputFormatClass(TextInputFormat.class);
   		job.setReducerClass(MyReduce.class);
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(Text.class);
   		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
   		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
   		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}