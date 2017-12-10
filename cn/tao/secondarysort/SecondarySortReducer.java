package cn.tao.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values,
			Reducer<DateTemperaturePair, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for(Text value:values){
			sb.append(value.toString());
			sb.append(",");
		}
		System.out.println(key.getYearMonth());
		System.out.println(sb.toString());
		context.write(key.getYearMonth(), new Text(sb.toString()));
	}
}
