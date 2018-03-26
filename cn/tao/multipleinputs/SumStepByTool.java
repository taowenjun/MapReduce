package cn.tao.multipleinputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.sound.midi.MidiDevice.Info;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *@author  Tao wenjun
 * 指定多个输入路径（数据格式有区别）的实现
 * file1：zhangsan@163.com    6000    0   2014-02-20
 * file2：zhangsan@163.com,6000,0,2014-02-20
 */

public class SumStepByTool extends Configured implements Tool{

	/**
	 * 处理file1的Mapper
	 */
	public static class SumStepByToolMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
		private InfoBean outBean = new InfoBean();
		private Text k = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields=line.split("\t");
			outBean.setAccount(fields[0]);
			//System.out.println(fields[0]+fields[1]);
			outBean.setIncome(Double.parseDouble(fields[1]));
			outBean.setExpense(Double.parseDouble(fields[2]));
			k.set(fields[0]);
			context.write(k, outBean);
		}
	}
	
	/**
	 * 处理file2的Mapper
	 */
	public static class SumStepByToolWithCommaMapper extends Mapper<LongWritable,Text,Text,InfoBean>{
		private InfoBean outBean = new InfoBean();
		private Text k = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			outBean.setAccount(fields[0]);
			outBean.setIncome(Double.parseDouble(fields[1]));
			outBean.setExpense(Double.parseDouble(fields[2]));
			k.set(fields[0]);
			context.write(k, outBean);
		}
	}
	
	/**
	 * Reducer
	 */
	public static class SumStepToolReducer extends Reducer<Text, InfoBean, Text, InfoBean>{
		private InfoBean outBean = new InfoBean();
	
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values, Reducer<Text, InfoBean, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			double incomeSum=0.0;
			double expenseSum=0.0;
			for(InfoBean bean:values){
				incomeSum+=bean.getIncome();
				expenseSum+=bean.getExpense();
			}
			outBean.setAccount("");
			outBean.setExpense(expenseSum);
            outBean.setIncome(incomeSum);
			context.write(key, outBean);
		}
	}
	
	/**
	 * Partitioner
	 */
	public static class SumStepToolPartitioner extends Partitioner<Text, InfoBean>{
		private static Map<String, Integer> accountMap = new HashMap<>();
		static{
			accountMap.put("zhangsan", 0);
			accountMap.put("lisi",2);
			accountMap.put("wangwu", 1);
		}
		@Override
		public int getPartition(Text key, InfoBean value, int numPartitions) {
			String keyString=key.toString();
			String name=keyString.substring(0, keyString.indexOf("@"));
			Integer part = accountMap.get(name);
			System.out.println(name);
			System.out.println(part);
			if(part==null){
				part=0;
			}
			return part;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());
		job.setJobName("SumStepByTool");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		
		job.setPartitionerClass(SumStepToolPartitioner.class);
		
		job.setReducerClass(SumStepToolReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		job.setNumReduceTasks(3);
		
		MultipleInputs.addInputPath(job, new Path("hdfs://10.108.21.2:9000/multimapper/data/info1/"), TextInputFormat.class,SumStepByToolMapper.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://10.108.21.2:9000/multimapper/data/info2/"), TextInputFormat.class,SumStepByToolWithCommaMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/multimapper/data/result"));
		return job.waitForCompletion(true)?0:-1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SumStepByTool(), args);
		System.exit(exitCode);
	}

}
