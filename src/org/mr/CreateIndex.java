package org.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreateIndex {
	public static class CreateIndexMapper extends
			Mapper<Object, Text, Text, Text> {
		private final static Text one = new Text("1");
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			InputSplit is = context.getInputSplit();
			String filename = ((FileSplit) is).getPath().getName();
			String itr[] = value.toString().split(",");
			for (int i = 0; i < itr.length; i++) {
				word.set(itr[i] + "^_^" + filename);
				context.write(word, one);
			}
		}
	}

	public static class CreateIndexCombiner extends
			Reducer<Text, Text, Text, Text> {
		private Text wordk = new Text();
		private Text wordv = new Text();
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String str[]=key.toString().split("\\^\\_\\^");
			int sum=0;
			for (Text val : values) {
				sum+=Integer.parseInt(val.toString());
			}
			wordk.set(str[0]);
			wordv.set(str[1]+"^_^"+sum);
			System.out.println(wordk.toString()+"  "+wordv.toString());
			context.write(wordk, wordv);
		}
	}

	public static class CreateIndexReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Map<String,Integer> map=new HashMap<String,Integer>();
			for (Text val : values) {
				String str[]=val.toString().split("\\^\\_\\^");
				Integer i=Integer.parseInt(str[1]);
				Integer j=map.get(str[0]);
				System.out.println(i);
				if(j!=null){
					j=i+j;
				}else{
					j=i;
				}
				map.put(str[0], j);
			}
			StringBuffer sb=new StringBuffer();
			for(String m:map.keySet()){
				sb.append(m+" "+map.get(m)+" ");
			}
			result.set(sb.toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/*.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "CreateIndex"); // 设置一个用户定义的job名称
		job.setJarByClass(CreateIndex.class);
		job.setMapperClass(CreateIndexMapper.class); // 为job设置Mapper类
		job.setCombinerClass(CreateIndexCombiner.class); // 为job设置Combiner类
		job.setReducerClass(CreateIndexReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
