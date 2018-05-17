package cn.tao.topn;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *@author  tao wenjun
 *@date 2018��5��17��
 *Top N Mapper
 *ʹ��TreeMap,��TreeMap�Ĵ�С����N���ʹ�TreeMap���Ƴ���С��key��
 */

public class TopNMapper extends Mapper<Text,Text,NullWritable,Text> {

	private SortedMap<Integer,String> top = new TreeMap<Integer,String>();
	private int N = 5;
	
	/**
	 * ���������ݽ��д��������뵽TreeMap�С�
	 */
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String keyAsString = key.toString();
		int valueAsInt = Integer.parseInt(value.toString());
		String compositeValue = valueAsInt+","+keyAsString;
		System.out.println(compositeValue);
		top.put(valueAsInt, compositeValue);
		if(top.size()>N){
			top.remove(top.firstKey());
		}
		
	}
	
	/**
	 * ��ʼ������������N
	 */
	@Override
	protected void setup(Mapper<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.N=context.getConfiguration().getInt("N", 5);
	}
	
	/**
	 * ������ľֲ�top N���
	 */
	@Override
	protected void cleanup(Mapper<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for(String value:top.values()){
			context.write(NullWritable.get(), new Text(value));
		}
	}
}
