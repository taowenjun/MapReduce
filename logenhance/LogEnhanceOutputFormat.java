package cn.tao.logenhance;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * maptask����reducetask���������ʱ���ȵ���OutputFormat��getRecordWriter�������õ�һ��RecordWriter
 * Ȼ���ٵ���RecordWriter��write()����������д��
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable>{

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		FileSystem fs=FileSystem.get(job.getConfiguration());
		Path enhancePath=new Path("/logenhance/log1/log.dat");
		Path tocrawlPath=new Path("/logenhance/tocrawl/url.dat");
		
		FSDataOutputStream enOs = fs.create(enhancePath);
		FSDataOutputStream tocrawlOs = fs.create(tocrawlPath);
		return new EnhanceRecordWriter(enOs,tocrawlOs);
	}

	/*
	 * �����Լ���RecordWriter
	 */
	static class EnhanceRecordWriter extends RecordWriter<Text,NullWritable>{
		FSDataOutputStream enOs = null;
		FSDataOutputStream tocrawlOs = null;
		
		public EnhanceRecordWriter(FSDataOutputStream enOs, FSDataOutputStream tocrawlOs) {
			super();
			this.enOs=enOs;
			this.tocrawlOs=tocrawlOs;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			if(key.toString().contains("tocrawl")){
				tocrawlOs.write(key.toString().getBytes());
			}else{
				enOs.write(key.toString().getBytes());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if(tocrawlOs!=null){
				tocrawlOs.close();
			}
			if(enOs!=null){
				enOs.close();
			}
		}		
	}
}
