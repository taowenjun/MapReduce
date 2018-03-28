package cn.tao.itemcf.recommend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *@author  Tao wenjun
 *ItemCF第三步：推荐
 */

public class Recommend {
	public static class RecommendMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		Text k = new Text();
		DoubleWritable v = new DoubleWritable();
		Map<String, Map<String,Double>> itemOccurenceMap = new HashMap<String,Map<String,Double>>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			/*
			 * setup:
			 * 将物品同现矩阵加载进内存：Map<String, Map<String,Double>>
			 */
			if(context.getCacheFiles()!=null&&context.getCacheFiles().length>0){
				String path = context.getLocalCacheFiles()[0].getName();
				
				//System.out.println("****************"+path);
				File file = new File(path);
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String s;
				while((s=bufferedReader.readLine())!=null){
					String[] strs = s.split("\t");
					if(strs.length!=2){
						continue;
					}
					String[] items = strs[0].split(":");
					String item1=items[0];
					String item2=items[1];
					Double perference = Double.parseDouble(strs[1]);
					//System.out.println(item1+" "+item2+" "+perference);
					Map<String,Double> itemMap;
					if(itemOccurenceMap.containsKey(item1)){
						itemMap=itemOccurenceMap.get(item1);
					}else{
						itemMap=new HashMap<String,Double>();
					}
					itemMap.put(item2, perference);
					itemOccurenceMap.put(item1, itemMap);
				}
				bufferedReader.close();
				fileReader.close();
			}
		}
		
		/*
		 * 计算所有商品中，用户没有评分的商品的得分
		 * 目标商品的同现矩阵与用户评分矩阵相乘
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs=value.toString().split("\t");
			String userId=strs[0];
			String temp=strs[1];
			for(Map.Entry<String, Map<String,Double>> entry:itemOccurenceMap.entrySet()){
				String targetId = entry.getKey();
				if(temp.contains(targetId)){
					continue;
				}
				double totalScore = 0.0;
				Map<String,Double> curMap = entry.getValue();
				String[] items=temp.split(",");
				for(String item:items){
					String[] tempArr=item.split(":");
					totalScore+=Double.parseDouble(tempArr[1])*curMap.getOrDefault(tempArr[0],0.0);
				}
				k.set(userId+":"+targetId);
				v.set(totalScore);
				context.write(k, v);
			}
		}
	}
	
	public static class RecommendReducer extends Reducer<Text, DoubleWritable, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			double total=0.0;
			for(DoubleWritable value:values){
				total+=value.get();
			}
			String[] strs = key.toString().split(":");
			k.set(strs[0]);
			v.set(strs[1]+":"+total);
			context.write(k, v);
		}
	}
}
