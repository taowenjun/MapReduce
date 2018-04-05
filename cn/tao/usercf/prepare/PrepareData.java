package cn.tao.usercf.prepare;

import java.io.IOException;
import java.util.HashMap;
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

/**
 *@author  Tao wenjun
 *Step 1：整理输入数据，为计算用户相似矩阵准备
 *最终输出：针对每商品  购买过他的用户对其的打分形成对  userid1,userid2  score1,score2
 */

public class PrepareData {
	/**
	 * PrepareDataMapper:
	 * 输入：原始数据：用户，商品，评分
	 * 输出：key:item  value:userid,score
	 */
	public static class PrepareDataMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line  = value.toString().trim();
			String[] arr = line.split(",");
			if(arr.length==3){
				k.set(arr[1]);
                v.set((arr[0]+","+arr[2]));
                context.write(k, v);
			}
		}
	}
	
	/**
	 * PrepareDataReducer:
	 * 输入：key:item values:一系列userid,score
	 * 输出：针对每个商品：userid1,userid2  score1,score2
	 */
	public static class PrepareDataReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String,String> map = new HashMap<>();
			for(Text value:values){
				String val = value.toString();
				String[] vals = val.split(",");
				if(vals.length>=2){
					map.put(vals[0],vals[1]);
				}
			}
			
			for(Map.Entry<String, String> entry1:map.entrySet()){
				String k1=entry1.getKey();
				String v1=entry1.getValue();
				for(Map.Entry<String, String> entry2:map.entrySet()){
					String k2 = entry2.getKey();
					String v2 = entry2.getValue();
					context.write(new Text(k1+","+k2), new Text(v1+","+v2));
				}
			}
		}
	}
	
	public static int run(Map<String, String> paths) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		String input=paths.get("step1_input");
		String output=paths.get("step1_output");
		
		job.setJarByClass(PrepareData.class);
		job.setMapperClass(PrepareDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PrepareDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
}
