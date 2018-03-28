package cn.tao.itemcf.scorematrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *@author  Tao wenjun
 *ItemCF第一步：计算用户评分矩阵
 */

public class UserScoreMatrix {
	/**
	 * Mapper：
	 * 将原始数据进行转换，以每行UserID为key，ItemId：Perference作为value输出
	 */
	public static class UserScoreMatrixMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] info = value.toString().split("\t");
			if(info.length==3){	
				k.set(info[0]);
				v.set(info[1]+":"+info[2]);
				context.write(k, v);
			}
		}
	}
	
	/**
	 * Reducer：
	 * 将map的输出聚合为用户评分矩阵输出
     * 将UserId相同的所有评分记录进行汇总拼接，输出的key仍然为1，value形如：101:5,102:3,103:2.5
	 */
	public static class UserScoreMatrixReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value:values){
				sb.append(value);
				sb.append(",");
			}
			String out = sb.toString();
			context.write(key, new Text(out.substring(0, out.length())));
		}
	}
}
