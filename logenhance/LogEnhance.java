package cn.tao.logenhance;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhance {

	public static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text k=new Text();
		IntWritable v=new IntWritable(1);
		
		HashMap<String, String> knowledgeMap=new HashMap<String,String>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			DBLoader.dbLoader(knowledgeMap);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			
			try {
				String url = fields[26];

				// 对这一行日志中的url去知识库中查找内容分析信息
				String content = knowledgeMap.get(url);

				// 根据内容信息匹配的结果，来构造两种输出结果
				String result = "";
				if (null == content) {
					// 输往待爬清单的内容
					result = url + "\t" + "tocrawl\n";
				} else {
					// 输往增强日志的内容
					result = line + "\t" + content + "\n";
				}

				context.write(new Text(result), NullWritable.get());
			} catch (Exception e) {

			}

		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.jop.tracker", "hdfs://10.108.21.2:9001");
		conf.set("fs.defaultFs", "hdfs://10.108.21.2:9000");
		Job job=Job.getInstance(conf);
		
	    job.setJarByClass(LogEnhance.class);
	    
	    job.setMapperClass(LogEnhanceMapper.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setOutputFormatClass(LogEnhanceOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job, new Path("hdfs://10.108.21.2:9000/logenhance/log"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/logenhance/out"));
	    
	    System.out.println(job.waitForCompletion(true)?0:1);
	}
}
