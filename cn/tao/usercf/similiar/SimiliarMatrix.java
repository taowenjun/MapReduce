package cn.tao.usercf.similiar;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.tao.usercf.prepare.PrepareData;
import cn.tao.usercf.prepare.PrepareData.PrepareDataMapper;
import cn.tao.usercf.prepare.PrepareData.PrepareDataReducer;

/**
 *@author  Tao wenjun
 *Step 2：计算用户相似矩阵
 *输入：Step 1处理后的数据  userid1,userid2  score1,score2
 *输出：用户相似矩阵  userid1,userid2  similarity
 */

public class SimiliarMatrix {
	/**
	 * SimiliarMatrixMapper:
	 * 输入：Step 1处理后的数据,购买过相同商品的用户具有一定的相似度
	 * 输出：userid1,userid2 score1,score2
	 */
	public static class SimiliarMatrixMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line  = value.toString().trim();
			String[] strs = line.split("\t");
			context.write(new Text(strs[0]), new Text(strs[1]));
		}
	}
	
	/**
	 * SimiliarMatrixReducer:
	 * 输入：userid1,userid2     一系列score1,score2
	 * 输出：用户相似度矩阵  userid1,userid2  similarity
	 */
	public static class SimiliarMatrixReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double sum=0.0;
			int num=0;
			for(Text value:values){
				String[] scs = value.toString().trim().split(",");
				if(scs.length<2){
					continue;
				}
				sum+=Math.pow((Double.parseDouble(scs[0])-Double.parseDouble(scs[1])), 2);
				num++;
 			}
			double similarity=0.0;
			if(sum>0.0000001){
				similarity=(double)num/(1+Math.sqrt(sum));
			}
			if(similarity>1){
				similarity=1;
			}
			context.write(key, new Text(String.format("%.7f", similarity)));
		}
	}
	
	public static int run(Map<String, String> paths) throws Exception, IOException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		String input=paths.get("step2_input");
		String output=paths.get("step2_output");
		
		job.setJarByClass(SimiliarMatrix.class);
		job.setMapperClass(SimiliarMatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(SimiliarMatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
}
