package cn.tao.flowsumsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * @author Tao wenjun
 * FlowBean:����������ʵ��
 */
public class FlowBean implements WritableComparable<FlowBean>{
	private long upFlow;
	private long downFlow;
	private long sum;
	
	//�����л�ʱ����Ҫ������ÿղι��캯��
	public FlowBean(){}
	
	public FlowBean(long upFlow,long downFlow){
		this.upFlow=upFlow;
		this.downFlow=downFlow;
		this.sum=this.upFlow+this.downFlow;
	}
	
	public void set(long upFlow,long downFlow){
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
	 * ���л�����
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sum);
	}

	/*
	 * �����л�����
	 * ע�ⷴ���л���˳�������л�˳��һ��
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

	@Override
	public int compareTo(FlowBean o) {
		
		return this.sum>o.sum?-1:1;
	}
}
