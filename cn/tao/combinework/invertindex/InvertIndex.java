package cn.tao.combinework.invertindex;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *@author  Tao wenjun
 *���������������������ŵ�һ�β�����
 */

public class InvertIndex {
	/*
	 * Mapper��ӳ��ÿ����
	 * �������--������   1
	 */
	static class InvertIndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text k=new Text();
		IntWritable v=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words=line.split(" ");
			FileSplit split = (FileSplit)context.getInputSplit();
			String fileName=split.getPath().getName();
			for(String word:words){
				k.set(word+"--"+fileName);
			    context.write(k, v);
			}
		}
	}
	
	/*
	 * Reducer��ͳ�����
	 */
	static class InvertIndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	/*
     * Mapper:�ʺͶ�Ӧ�����¼�������������
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
	 * Reducer:ͳ��ÿ���ʵĳ������
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
	
	public static void main(String[] args) throws Exception, IOException {
		/**  Step One */
		//configuration
		Configuration conf=new Configuration();
		//job
		Job job=Job.getInstance(conf);
				
		job.setJarByClass(InvertIndex.class);
		//Mapper
		job.setMapperClass(InvertIndexStepOneMapper.class);
		//Reducer
		job.setReducerClass(InvertIndexStepOneReducer.class);
		//Map output key����
		job.setMapOutputKeyClass(Text.class);
		//Reduce output value����
		job.setMapOutputValueClass(IntWritable.class);
		//Output key����
		job.setOutputKeyClass(Text.class);
		//Output value����
		job.setOutputValueClass(IntWritable.class);
		//Input  path
		FileInputFormat.addInputPath(job, new Path("hdfs://10.108.21.2:9000/invertindex/data"));
	    //Output path
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/invertindex/re"));
				
		System.out.println(job.waitForCompletion(true)?0:1);
		
		
		/**  Step two */
		//configuration
		Configuration conf2=new Configuration();
	    Job job2=Job.getInstance(conf2);
				
		job2.setJarByClass(InvertIndex.class);
				
		//Mapper
		job2.setMapperClass(InvertIndexStepTwoMapper.class);
		//Reducer
		job2.setReducerClass(InvertIndexStepTwoReducer.class);
		//Map output key����
		job2.setMapOutputKeyClass(Text.class);
		//Map output value����
		job2.setMapOutputValueClass(Text.class);
		//output key����
		job2.setOutputKeyClass(Text.class);
		//output value����
		job2.setOutputValueClass(Text.class);
		//Input PATH
		FileInputFormat.addInputPath(job2, new Path("hdfs://10.108.21.2:9000/invertindex/re"));
		//Output PATH
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://10.108.21.2:9000/invertindex/reii"));
				
		System.out.println(job2.waitForCompletion(true)?0:1);
	}
}
