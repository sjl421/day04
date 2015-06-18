package org.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataDuplicateRemove {
	public static class DataDuplicateRemoveMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++) {
				word.set(itr[i]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class DataDuplicateRemoveReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/test.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "DataDuplicateRemove"); // 设置一个用户定义的job名称
		job.setJarByClass(DataDuplicateRemove.class);
		job.setMapperClass(DataDuplicateRemoveMapper.class); // 为job设置Mapper类
		job.setCombinerClass(DataDuplicateRemoveReducer.class); // 为job设置Combiner类
		job.setReducerClass(DataDuplicateRemoveReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); //  
		job.setOutputValueClass(NullWritable.class); //  
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}

}
