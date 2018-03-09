package cn.tao.invertindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * @Author:TaoWenjun
 * <����������һ��>
 * ����ÿ������ÿƪ�����г��ֵĴ���
 * ��������ʽΪ ����--������    ����
 * �������Էŵ�һ��
 */
public class InvertIndexStepOne {
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
	
	public static void main(String[] args) throws Exception {
		//configuration
		Configuration conf=new Configuration();
		//job
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(InvertIndexStepOne.class);
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
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/invertindex/result"));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
