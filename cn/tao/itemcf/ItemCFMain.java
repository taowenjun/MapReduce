package cn.tao.itemcf;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import cn.tao.itemcf.itemoccurence.ItemOccurence.ItemOccurenceMapper;
import cn.tao.itemcf.itemoccurence.ItemOccurence.ItemOccurenceReducer;
import cn.tao.itemcf.recommend.Recommend.RecommendMapper;
import cn.tao.itemcf.recommend.Recommend.RecommendReducer;
import cn.tao.itemcf.scorematrix.UserScoreMatrix.UserScoreMatrixMapper;
import cn.tao.itemcf.scorematrix.UserScoreMatrix.UserScoreMatrixReducer;


/**
 *@author  Tao wenjun
 *ItemCF
 */

public class ItemCFMain extends Configured implements Tool{
    private static String URL="hdfs://10.108.21.2:9000/recommand/ItemCF/";
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ItemCFMain(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		//1°¢º∆À„∆¿∑÷æÿ’Û
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance();
		job1.setJarByClass(ItemCFMain.class);
		job1.setMapperClass(UserScoreMatrixMapper.class);
		job1.setReducerClass(UserScoreMatrixReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		String score="score";
		String scorematrix="scorematrix";
		String occurence="occurence";
		FileInputFormat.addInputPath(job1, new Path(URL+score));
		FileOutputFormat.setOutputPath(job1, new Path(URL+scorematrix));
		
		if(job1.waitForCompletion(true)){
			Job job2 = Job.getInstance();
			job2.setJarByClass(ItemCFMain.class);
			job2.setMapperClass(ItemOccurenceMapper.class);
			job2.setReducerClass(ItemOccurenceReducer.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			
			FileInputFormat.addInputPath(job2, new Path(URL+scorematrix));
			FileOutputFormat.setOutputPath(job2, new Path(URL+occurence));
			
			if(job2.waitForCompletion(true)){
				Job job3 = Job.getInstance();
				job3.setJarByClass(ItemCFMain.class);
				job3.setMapperClass(RecommendMapper.class);
				job3.setReducerClass(RecommendReducer.class);
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(DoubleWritable.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				
				job3.addCacheFile(new URI(URL+occurence+"/part-r-00000"));
				FileInputFormat.addInputPath(job3, new Path(URL+scorematrix));
				FileOutputFormat.setOutputPath(job3, new Path(URL+"result"));
				
				System.out.println(job3.waitForCompletion(true)?0:1);
			}
			System.out.println(job2.waitForCompletion(true)?0:1);
		}
		
		return job1.waitForCompletion(true)?0:1;
	}

}
