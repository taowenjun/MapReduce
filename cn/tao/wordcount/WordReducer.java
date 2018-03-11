package cn.tao.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private int sum;
	private IntWritable result=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		sum=0;
		
		for(IntWritable num:value){
			sum+=num.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
