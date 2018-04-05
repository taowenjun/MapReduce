package cn.tao.usercf.recommend;

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
 * @author  Tao wenjun
 * Step 5：根据推荐矩阵给用户推荐前3个商品
 * 输入：userId itemID，score
 * 输出：userId <itemId[score],>
 */

public class RecommendGoods {
	/**
	 * RecommendGoodsMapper:
	 * 输入：
	 * 输出：
	 */
	public static class RecommendGoodsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t|,");
			if(strs.length>=3){
				context.write(new Text(strs[0]), new Text(strs[1]+","+strs[2]));
			}
		}
	}
	
	/**
	 * RecommendGoodsReducer:
	 * 输入：
	 * 输出：
	 */
	public static class RecommendGoodsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String,Double> map = new HashMap<>();
			int goodsNum=3;
			for(Text value:values){
				String[] strs = value.toString().trim().split(",");
				if(strs.length<2){
					continue;
				}
				map.put(strs[0], Double.parseDouble(strs[1]));
			}
			List<Map.Entry<String, Double>> list = new ArrayList<>(map.entrySet());
			Collections.sort(list,new Comparator<Map.Entry<String, Double>>() {

				@Override
				public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
					if(o1.getValue()<o2.getValue()){
						return 1;
					}else{
						return -1;
					}
				}
			});
			if(list.size()==0){
				context.write(key, new Text("none"));
			}else{
				String v = "";
				for(int i=0;i<goodsNum&&i<list.size();i++){
					v+=","+list.get(i).getKey()+"["+list.get(i).getValue()+"]";
				}		
				context.write(key, new Text(v.substring(1)));
			}
		}
	}

	public static int run(Map<String, String> paths) throws IOException, Exception, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		String input=paths.get("step5_input");
		String output=paths.get("step5_output");
		
		job.setJarByClass(RecommendGoods.class);
		job.setMapperClass(RecommendGoodsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(RecommendGoodsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
}
