package cn.tao.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author tao wenjun
 * @date 2018��5��17�� Top N Reducer
 *
 */

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

	private int N = 5; // default
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

	/**
	 * ����N
	 */
	@Override
	protected void setup(Reducer<NullWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 5);
	}

	/**
	 * reduce������ȫ��Top N
	 */
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] keyValue=value.toString().trim().split(",");
			int k=Integer.parseInt(keyValue[0]);
			String v=keyValue[1];
			top.put(k, v);
			
			//top�Ĵ�С����N
			if(top.size()>N){
				top.remove(top.firstKey());
			}
		}

		// emit final top N
		List<Integer> keys = new ArrayList<>(top.keySet());
		for(int i=0;i<keys.size();i++){
			context.write(new IntWritable(keys.get(i)), new Text(top.get(keys.get(i))));
		}
	}
}
