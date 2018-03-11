package cn.tao.wholefiles;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Tao wenjun
 * 整个文件作为一个输入块
 * RecordReader的核心工作逻辑：
 * 通过nextKeyValue()方法去读取数据构造将返回的key  value
 * 通过getCurrentKey()和getCurrentValue()来返回上面构造好的key和value
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable>{

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader reader=new WholeFileRecordReader();
		reader.initialize(split,context);
		return reader;
	}
}

class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable>{
	private FileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value=new BytesWritable();
	private boolean processed=false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit=(FileSplit)split;
		this.conf=context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!processed){
			byte[] contents=new byte[(int)fileSplit.getLength()];
			Path file=fileSplit.getPath();
			FileSystem fs=file.getFileSystem(conf);
			FSDataInputStream in=null;
			try{
				in=fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents,0,contents.length);
			}finally{
				IOUtils.closeStream(in);
			}
			processed=true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {		
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed?1.0f:0.0f;
	}

	@Override
	public void close() throws IOException {
		
	}
	
}
