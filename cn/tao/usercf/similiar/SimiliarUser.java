package cn.tao.usercf.similiar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
 *Step 3：根据相似矩阵找出与当前用户相似度最高的前2个用户
 *输入：Step 2输出结果  userid1,userid2  similarity
 *输出：每个用户与其相似度最高的前N个用户
 */

public class SimiliarUser {
	/**
	 * SimiliarUserMapper:
	 * 输入：Step 2输出结果  userid1,userid2  similarity
	 * 输出：userid1    userid2,similarity
	 */
	public static class SimiliarUserMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t|,");
			if(strs.length>=3){
				System.out.println(strs[1]+","+strs[2]);
				context.write(new Text(strs[0]), new Text(strs[1]+","+strs[2]));
			}
		}
	}
	
	/**
	 * SimiliarUserReducer:
	 * 输入：userid1    一系列userid2,similarity
	 * 输出：userid1 similarity最大的N个userid
	 */
	public static class SimiliarUserReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<>();
			int N=2;
			for(Text value:values){
				String[] strs = value.toString().trim().split(",");
				map.put(strs[0], Double.parseDouble(strs[1]));
			}
			List<Map.Entry<String, Double>> list = new ArrayList<>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {

				@Override
				public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
					if(o1.getValue()<o2.getValue()){
						return 1;
					}else{
						return -1;
					}
				}
			});
			String v = "";
			System.out.println("lise size:"+list.size());
			for(int i=0;i<N&&i<list.size();i++){
				Map.Entry<String,Double> entry=list.get(i);
				v+=(","+entry.getKey()+","+entry.getValue());
			}
			context.write(key, new Text(v.substring(1)));
		}
	}

	public static int run(Map<String, String> paths) throws Exception, IOException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		String input=paths.get("step3_input");
		String output=paths.get("step3_output");
		
		job.setJarByClass(SimiliarUser.class);
		job.setMapperClass(SimiliarUserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(SimiliarUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	 private static String URL="hdfs://10.108.21.2:9000/recommend/UserCF/";
	public static void main(String[] args) throws Exception {
			Map<String,String> map = new HashMap<>();
			map.put("step1_input", URL+"data/");
			map.put("step1_output",URL+"step1");
			map.put("step2_input",map.get("step1_output"));
			map.put("step2_output",URL+"/step2");
			map.put("step3_input",map.get("step2_output"));
			map.put("step3_output",URL+"step3");
			map.put("step4_input",map.get("step3_output"));
			map.put("step4_output",URL+"step4");
			map.put("step5_input",map.get("step4_output"));
			map.put("step5_output",URL+"step5");
		run(map);
	}
}
