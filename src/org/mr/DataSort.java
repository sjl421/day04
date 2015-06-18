package org.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataSort {
	public static class DataSortMapper extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritable word =new IntWritable(Integer.parseInt(value.toString()));
			context.write(word, one);
		}
	}

	public static class DataSortReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for(IntWritable i:values){
				context.write(key, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/test.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "DataSort"); // 设置一个用户定义的job名称
		job.setJarByClass(DataSort.class);
		job.setMapperClass(DataSortMapper.class); // 为job设置Mapper类
		job.setReducerClass(DataSortReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(IntWritable.class); //  
		job.setOutputValueClass(IntWritable.class); //  
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}

}
