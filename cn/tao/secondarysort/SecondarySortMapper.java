package cn.tao.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {
	private final DateTemperaturePair pair = new DateTemperaturePair();
    private final Text theTemp = new Text();
    
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, DateTemperaturePair, Text>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] tokens=line.split(",");
		
		pair.setYearMonth(tokens[0]+tokens[1]);
		pair.setDay(tokens[2]);
		pair.setTemperature(Integer.parseInt(tokens[3]));
		theTemp.set(tokens[3]);
		context.write(pair, theTemp);
	}
}
