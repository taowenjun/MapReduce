package cn.tao.flowsumsort;

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
 * 对统计的流量结果进行排序
 */
public class FlowCountSort {

	static class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
		FlowBean bean=new FlowBean();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] fields=line.split("\t");
			String phoneNum=fields[0];
			long upFlow=Long.parseLong(fields[1]);
			long downFlow=Long.parseLong(fields[2]);
			bean.set(upFlow,downFlow);
			v.set(phoneNum);
			context.write(bean, v);
		}
	}
	
	static class FlowCountSortReducer extends Reducer<FlowBean,Text,Text,FlowBean>{
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//while(values.iterator().hasNext()){
				context.write(values.iterator().next(), bean);
			//}
		}
	}
	
	public static void main(String[] args) throws Exception, IOException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowCountSort.class);
		
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.108.21.2:9000/phoneflow/output"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/phoneflow/outputsort"));
	    System.out.println(job.waitForCompletion(true)?0:1);
	}
}
