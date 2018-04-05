package cn.tao.usercf.recommend;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.tao.usercf.similiar.SimiliarMatrix;
import cn.tao.usercf.similiar.SimiliarMatrix.SimiliarMatrixMapper;
import cn.tao.usercf.similiar.SimiliarMatrix.SimiliarMatrixReducer;

/**
 *@author  Tao wenjun
 *Step 4：根据相似用户计算推荐矩阵
 */

public class RecommendMatrix {
	/**
	 * RecommendMatrixMapper:
	 * 输入：SimilarUser 和 原始数据
	 * 输出：用户推荐矩阵
	 */
	public static class RecommendMatrixMapper extends Mapper<LongWritable, Text, Text, Text>{
		private String flag;
		private int itemNum=7;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)context.getInputSplit();
			flag = split.getPath().toString();
			System.out.println(flag);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int itemIndex = 100;
			String line = value.toString().trim();
			String[] tokens = line.split("\t|,");
			if(flag.contains("step3")&&tokens.length>=3){
                Text v = null;
                if(tokens.length<5){
                	v=new Text("A:"+tokens[0]+","+tokens[1]);  
                }else{
                	v=new Text("A:"+tokens[0]+","+tokens[1]+","+tokens[3]);          	
                }
				for(int i=1;i<=itemNum;i++){
					Text k = new Text(Integer.toString(itemIndex+i));
					context.write(k, v);
				}
			}else{
				Text k = new Text(tokens[1]);
				Text v = new Text("B:"+tokens[0]+","+tokens[2]);
				context.write(k, v);
			}
		}
	}
	
	/**
	 * RecommendMatrixReducer:
	 * 输入：itemId <A:... B:...>
	 * 输出：userId itemID，score
	 */
	public static class RecommendMatrixReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String,String> mapA = new HashMap<String,String>();
			Map<String,Double> mapB = new HashMap<>();
			for(Text value:values){
				String line = value.toString().trim();
				if(line.startsWith("A:")){
					String[] kv = line.substring(2).split(",");
					if(kv.length>=3){
						mapA.put(kv[0], kv[1]+","+kv[2]);		
					}else if(kv.length>=2){
						mapA.put(kv[0], kv[1]);
					}else{
						continue;
					}
				}else if(line.startsWith("B:")){
					String[] kv = line.substring(2).split(",");
					mapB.put(kv[0], Double.parseDouble(kv[1]));
				}
			}
			for(Map.Entry<String, String> entry1:mapA.entrySet()){
				String userId = entry1.getKey();
				String[] users = entry1.getValue().toString().trim().split(",");
				if(users.length==2){
					double score=(mapB.getOrDefault(users[0], 0.0)+mapB.getOrDefault(users[1], 0.0))/2;
					context.write(new Text(userId), new Text(key+","+score));
				}else if(users.length==1){
					double score=mapB.getOrDefault(users[0], 0.0);
					context.write(new Text(userId), new Text(key+","+score));
				}else{
					continue;
				}
			}
		}
	}

	public static int run(Map<String, String> paths) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		String input1=paths.get("step4_input");
		String input2=paths.get("step1_input");
		String output=paths.get("step4_output");
		
		job.setJarByClass(RecommendMatrix.class);
		job.setMapperClass(RecommendMatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(RecommendMatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path[]{new Path(input1),new Path(input2)});
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
}
