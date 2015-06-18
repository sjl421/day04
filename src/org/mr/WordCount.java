package org.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++)
			{
				word.set(itr[i]);
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/LICENSE.txt", "hdfs://master:9000/out/" };
		Job job = new Job(conf, "word count"); // 设置一个用户定义的job名称
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class); // 为job设置Mapper类
		job.setCombinerClass(IntSumReducer.class); // 为job设置Combiner类
		job.setReducerClass(IntSumReducer.class); // 为job设置Reducer类

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class); // 设置map输出的key类
		job.setOutputValueClass(IntWritable.class); // 设置map输出的value类

		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}

}
