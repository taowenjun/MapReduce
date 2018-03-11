package cn.tao.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration);
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(WordReducer.class);
		job.setReducerClass(WordReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		String[] path=new String[]{"hdfs://10.108.21.2:9000/wc/input","hdfs://10.108.21.2:9000/wc/output"};
	    ChainMapper<Text, Text, Text, Text> chainMapper = new ChainMapper();
	    ChainReducer redu=new ChainReducer();
        //ChainMapper.addMapper(job, klass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, mapperConf);
		FileInputFormat.setInputPaths(job, path[0]);
		FileOutputFormat.setOutputPath(job, new Path(path[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}
}
