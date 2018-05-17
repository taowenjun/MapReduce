package cn.tao.topn;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *@author  tao wenjun
 *@date 2018��5��17��
 *Top N Driver
 */

public class TopNDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		
		int N = Integer.parseInt(args[0]);
		job.getConfiguration().setInt("N", N);
		job.setJobName("TopNDriver");
		
		job.setJarByClass(TopNDriver.class);
		
		//�������������ʽ����
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		//��֤ȫ��top N
		job.setNumReduceTasks(1);
		
		//����Map���key,value����
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//�������key��value����
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		//�����������·��
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		boolean status = job.waitForCompletion(true);
		return status?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		//������
		if(args.length!=3){
			System.out.println("����������");
			System.exit(1);
		}
		int returnStatus=ToolRunner.run(new TopNDriver(), args);
		System.exit(returnStatus);
	}

}
