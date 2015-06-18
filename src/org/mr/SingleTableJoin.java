package org.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTableJoin {
	public static class DataGroupingMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text res = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split(",");
			word.set(itr[2]);
			res.set(itr[1]+"_l");
			context.write(word, res);
			word.set(itr[0]);
			res.set(itr[1]+"_r");
			context.write(word, res);
		}
	}

	public static class DataGroupingReducer extends
			Reducer<Text, Text, NullWritable,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			List<String> vl = new ArrayList<String>();
			List<String> kl = new ArrayList<String>();
			for (Text val : values) {
				String s = val.toString();
				if (s.contains("_l")) {
					kl.add(s.split("_")[0]);
				} else {
					vl.add(s.split("_")[0]);
				}
			}
			for (int j = 0; j < kl.size(); j++) {
				for (int i = 0; i < vl.size(); i++) {
					result.set(kl.get(j) + "," + vl.get(i));
					context.write(NullWritable.get(), result);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/test.txt",
				"hdfs://master:9000/out/" };
		Job job = new Job(conf, "DataGrouping"); // 设置一个用户定义的job名称
		job.setJarByClass(SingleTableJoin.class);
		job.setMapperClass(DataGroupingMapper.class); // 为job设置Mapper类
		job.setReducerClass(DataGroupingReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); //  
		job.setOutputValueClass(Text.class); //  
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
