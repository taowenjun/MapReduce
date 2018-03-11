package cn.tao.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one=new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String input = value.toString();
		input=input.replaceAll("[\\p{Punct}]+", " ");
		String[] words=input.split(" ");
		for(String word:words){
			context.write(new Text(word), one);
		}
	}
}
