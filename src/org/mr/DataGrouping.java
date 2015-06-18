package org.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataGrouping {
	public static class DataGroupingMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text res = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split(",");
			word.set(itr[0]);
			res.set(itr[1]+":"+itr[2]);
			context.write(word, res);
		}
	}

	public static class DataGroupingReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			StringBuffer sb=new StringBuffer();
			for (Text val : values) {
				String s=val.toString().split(":")[1];
				sum+=Integer.parseInt(s);
				sb.append(s+",");
			}
			sb.append(sum);
			result.set(sb.toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/test.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "DataGrouping"); // 设置一个用户定义的job名称
		job.setJarByClass(DataGrouping.class);
		job.setMapperClass(DataGroupingMapper.class); // 为job设置Mapper类
		job.setReducerClass(DataGroupingReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); //  
		job.setOutputValueClass(Text.class); //  
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
