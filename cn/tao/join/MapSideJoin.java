package cn.tao.join;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * @Author:TaoWenjun
 * @CreateTime:2018/03/08
 * <Map��join> ����Map�˵�setup�׶ν������ļ����ص�HashMap�У�map()��������key��ѯHashMap�е�ֵ������join
 */

public class MapSideJoin {

	static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Map<String,String> pdInfoMap=new HashMap<String, String>();
		
		Text k=new Text();
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
			String line;
			while(StringUtils.isNotEmpty(line=reader.readLine())){
				String[] fields=line.split(",");
				pdInfoMap.put(fields[0], fields[1]);
			}
			reader.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String orderLine=value.toString();
			String[] fields = orderLine.split("\t");
			String pdName = pdInfoMap.get(fields[1]);
			
			k.set(orderLine + "\t"+pdName);
			context.write(k, NullWritable.get());
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.108.21.2:9000/join/data"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/join/result"));
		
		//ָ����Ҫ����һ���ļ������е�maptask���нڵ㹤��Ŀ¼
		/*job.addArchiveToClassPath(archive);*/  //����jar����task���нڵ��classpath��
		/*job.addCacheArchive(uri);*/   //����ѹ�����ļ���task���нڵ�Ĺ���Ŀ¼
		/*job.addCacheFile(uri);*/  //������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼
		/*job.addFileToClassPath(file);*/  //������ͨ�ļ���task���нڵ��classpath��
		
		job.addCacheFile(new URI("file:///E:/mapjoincache/pdts.txt"));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
