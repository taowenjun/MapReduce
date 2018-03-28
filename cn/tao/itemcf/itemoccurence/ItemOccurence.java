package cn.tao.itemcf.itemoccurence;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *@author  Tao wenjun
 *ItemCF�ڶ�����������Ʒ���־���
 */

public class ItemOccurence {
	/**
	 * Mapper:
	 * �����ǵ�һ�������
     * �����keyΪƫ�����������valueΪUserId+�Ʊ��+ItermId1:Perference1,ItermId2:Perference2�� 
     * �����value�У�UserId��Perference�ǲ���Ҫ���ĵģ��۲���Ʒ��ͬ�־���
     * map�׶εĹ������ǽ�ÿ�а�����ItermId������������ȫ���������Ϊkey�����ÿ��key��value��Ϊ1�� 
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
	 * ����key��value�����ۼ����
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
