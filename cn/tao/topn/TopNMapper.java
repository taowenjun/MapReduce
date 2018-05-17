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
 *@date 2018年5月17日
 *Top N Mapper
 *使用TreeMap,当TreeMap的大小超过N，就从TreeMap中移除最小的key。
 */

public class TopNMapper extends Mapper<Text,Text,NullWritable,Text> {

	private SortedMap<Integer,String> top = new TreeMap<Integer,String>();
	private int N = 5;
	
	/**
	 * 对输入数据进行处理，并加入到TreeMap中。
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
	 * 初始化函数，设置N
	 */
	@Override
	protected void setup(Mapper<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.N=context.getConfiguration().getInt("N", 5);
	}
	
	/**
	 * 将处理的局部top N输出
	 */
	@Override
	protected void cleanup(Mapper<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for(String value:top.values()){
			context.write(NullWritable.get(), new Text(value));
		}
	}
}
