package org.mr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopK {
	public static class TopKMapper extends
			Mapper<Object, Text, NullWritable, LongWritable> {
		public static final int K = 3;
		private TreeMap<Long, Long> tm = new TreeMap<Long, Long>();

		protected void map(Object key, Text value, Context context)
				throws IOException {
			try {
				long k = Integer.parseInt(value.toString());
				tm.put(k, k);
				if (tm.size() > K) {
					tm.remove(tm.firstKey());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Long text : tm.values()) {
				context.write(NullWritable.get(), new LongWritable(text));
			}
		}
	}

	public static class TopKReducer extends
			Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
		public static final int K = 3;
		private TreeMap<Long, Long> mt = new TreeMap<Long, Long>();

		protected void reduce(NullWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			for (LongWritable value : values) {
				mt.put(value.get(), value.get());
				if (mt.size() > K) {
					mt.remove(mt.firstKey());
				}
			}
			for (Long val : mt.descendingKeySet()) {
				context.write(NullWritable.get(), new LongWritable(val));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/test.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "TopK"); // 设置一个用户定义的job名称
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class); // 为job设置Mapper类
		job.setReducerClass(TopKReducer.class); // 为job设置Reducer类
		job.setNumReduceTasks(1);//设置reduce个数
		job.setOutputKeyClass(NullWritable.class); //  
		job.setOutputValueClass(LongWritable.class); //  
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
