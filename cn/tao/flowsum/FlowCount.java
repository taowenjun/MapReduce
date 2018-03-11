package cn.tao.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * @author Tao wenjun
 * 统计手机号的上下行流量
 */
public class FlowCount {

	public static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//提取String，并切分字段
			String[] fields = value.toString().split("\t");
			//提取手机号
			String phone=fields[1];
			//提取上下行流量
			long upFlow=Long.parseLong(fields[fields.length-3]);
			long downFlow=Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(phone),new FlowBean(upFlow,downFlow));
		}
	}
	
	public static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long sum_upFlow=0;
			long sum_downFlow=0;
			for(FlowBean bean:values){
				sum_upFlow+=bean.getUpFlow();
				sum_downFlow+=bean.getDownFlow();
			}
			context.write(key, new FlowBean(sum_upFlow,sum_downFlow));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.108.21.2:9000/phoneflow/data"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/phoneflow/output"));
	    System.out.println(job.waitForCompletion(true)?0:1);
	}

}
