package cn.tao.provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.tao.flowsum.FlowBean;

/*
 * @author Tao wenjun
 * ��дPartitioner���ͣ������ֻ���ǰ׺���ֱ��������Ӧ������ļ���
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> provinceDict=new HashMap<String,Integer>();
	static{
		provinceDict.put("135", 0);
		provinceDict.put("136", 1);
		provinceDict.put("137", 2);
		provinceDict.put("138", 3);
		provinceDict.put("139", 4);
		provinceDict.put("182", 5);
		provinceDict.put("841", 5);
		provinceDict.put("159", 4);
		provinceDict.put("150", 3);
		provinceDict.put("134", 2);
		provinceDict.put("183", 1);
	}
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer provinceId=provinceDict.get(prefix);
		
		return provinceId==null?4:provinceId;
	}
}
