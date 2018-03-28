package cn.tao.itemcf.itemoccurence;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *@author  Tao wenjun
 *ItemCF第二步：计算物品共现矩阵
 */

public class ItemOccurence {
	/**
	 * Mapper:
	 * 输入是第一步的输出
     * 输入的key为偏移量，输入的value为UserId+制表符+ItermId1:Perference1,ItermId2:Perference2… 
     * 输入的value中，UserId和Perference是不需要关心的，观察物品的同现矩阵
     * map阶段的工作就是将每行包含的ItermId都解析出来，全排列组合作为key输出，每个key的value记为1。 
	 */
	public static class ItemOccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\t");
			if(arr.length!=2){
				return;
			}
			String[] strs = arr[1].split(",");
			for(int i=0;i<strs.length;i++){
				String item1 = strs[i].split(":")[0];
				for(int j=0;j<strs.length;j++){
					String item2 = strs[j].split(":")[0];
					context.write(new Text(item1+":"+item2), one);
				}
			}
		}
	}
	
	/**
	 * Reducer:
	 * 根据key对value进行累加输出
	 */
	public static class ItemOccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		IntWritable count = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			count.set(sum);
			context.write(key, count);
		}
	}
}
