package cn.tao.invertindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.tao.invertindex.InvertIndexStepOne.InvertIndexStepOneMapper;
import cn.tao.invertindex.InvertIndexStepOne.InvertIndexStepOneReducer;
/*
 * @Author:TaoWenjun
 * <倒排索引第二步>
 * 倒排结果：一个词分别在出现过该词文章中的次数
 */
public class InvertIndexStepTwo {
    /*
     * Mapper:词和对应的文章及次数次数分离
     * 
     */
	static class InvertIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("--");
			context.write(new Text(fields[0]), new Text(fields[1]));
		}
	}
	
	/*
	 * Reducer:统计每个词的出现情况
	 * 
	 */
	static class InvertIndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb=new StringBuilder();
			for(Text text:values){
				sb.append(text.toString()).append(";");
			}
			v.set(sb.toString());
			context.write(key,v);
		}
	}
	
	public static void main(String[] args) throws Exception {
		//configuration
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(InvertIndexStepTwo.class);
		
		//Mapper
		job.setMapperClass(InvertIndexStepTwoMapper.class);
		//Reducer
		job.setReducerClass(InvertIndexStepTwoReducer.class);
		//Map output key类型
		job.setMapOutputKeyClass(Text.class);
		//Map output value类型
		job.setMapOutputValueClass(Text.class);
		//output key类型
		job.setOutputKeyClass(Text.class);
		//output value类型
		job.setOutputValueClass(Text.class);
		//Input PATH
		FileInputFormat.addInputPath(job, new Path("hdfs://10.108.21.2:9000/invertindex/result"));
		//Output PATH
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/invertindex/result1"));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}
}
