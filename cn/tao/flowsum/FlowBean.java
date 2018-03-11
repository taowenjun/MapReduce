package cn.tao.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * @author Tao wenjun
 * FlowBean:上下行流量实体
 */
public class FlowBean implements Writable{
	private long upFlow;
	private long downFlow;
	private long sum;
	
	//反序列化时，需要反射调用空参构造函数
	public FlowBean(){}
	
	public FlowBean(long upFlow,long downFlow){
		this.upFlow=upFlow;
		this.downFlow=downFlow;
		this.sum=this.upFlow+this.downFlow;
	}
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSum() {
		return sum;
	}

	public void setSum(long sum) {
		this.sum = sum;
	}

	/*
	 * 序列化方法
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sum);
	}

	/*
	 * 反序列化方法
	 * 注意反序列化的顺序与序列化顺序一致
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sum=in.readLong();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upFlow+"\t"+downFlow+"\t"+sum;
	}
}
