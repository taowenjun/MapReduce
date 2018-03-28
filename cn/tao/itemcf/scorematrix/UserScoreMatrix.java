package cn.tao.itemcf.scorematrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *@author  Tao wenjun
 *ItemCF��һ���������û����־���
 */

public class UserScoreMatrix {
	/**
	 * Mapper��
	 * ��ԭʼ���ݽ���ת������ÿ��UserIDΪkey��ItemId��Perference��Ϊvalue���
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
	 * Reducer��
	 * ��map������ۺ�Ϊ�û����־������
     * ��UserId��ͬ���������ּ�¼���л���ƴ�ӣ������key��ȻΪ1��value���磺101:5,102:3,103:2.5
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
