package cn.tao.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortDriver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondarySortDriver.class);
		job.setJobName("SecondarySortDriver");
		
    		// args[0] = input directory
    		// args[1] = output directory
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.108.21.2:9000/SecondarySort/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.108.21.2:9000/SecondarySort/output2"));

		job.setOutputKeyClass(DateTemperaturePair.class);
		job.setOutputValueClass(Text.class);
		
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);		
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupComparator.class);
                                      
        System.out.println(job.waitForCompletion(true));
	}

}
