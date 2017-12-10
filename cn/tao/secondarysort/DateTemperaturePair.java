package cn.tao.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

	private final Text yearMonth = new Text();
	private final Text day = new Text();
	private final IntWritable temperature = new IntWritable();
	
	public DateTemperaturePair(){
		
	}
	
	public DateTemperaturePair(String yearMonth,String day,int temp){
		this.yearMonth.set(yearMonth);
		this.day.set(day);
		this.temperature.set(temp);
	}
	
	public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }
	
	@Override
	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		day.write(out);
		temperature.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		day.readFields(in);
		temperature.readFields(in);
	}

	@Override
	public int compareTo(DateTemperaturePair o) {
		int compareValue = this.yearMonth.compareTo(o.getYearMonth());
		if(compareValue==0){
			compareValue = temperature.compareTo(o.getTemperature());
		}
		return -1*compareValue;
	}

	public Text getYearMonthDay(){
	    return new Text(yearMonth.toString()+day.toString());	
	}
	
	public Text getYearMonth() {
		return yearMonth;
	}

	public Text getDay() {
		return day;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setYearMonth(String yMonth){
		yearMonth.set(yMonth);
	}
	
	public void setDay(String dayStr){
		day.set(dayStr);
	}
	
	public void setTemperature(int temp){
		temperature.set(temp);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this==obj){
			return true;
		}
		if(obj==null||getClass()!=obj.getClass()){
			return false;
		}
		DateTemperaturePair pair = (DateTemperaturePair)obj;
		if(this.temperature!=null?!this.temperature.equals(pair.temperature):pair.temperature!=null){
			return false;
		}
		if(this.yearMonth!=null?!this.yearMonth.equals(pair.yearMonth):pair.yearMonth!=null){
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		int result=yearMonth!=null?yearMonth.hashCode():0;
		result=31*result+(temperature!=null?temperature.hashCode():0);
		return result;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("DateTemperaturePair{yearMonth=");
		sb.append(yearMonth);
		sb.append(",day=");
		sb.append(day);
		sb.append(",temperature=");
		sb.append(temperature);
		sb.append("}");
		return sb.toString();
	}
}
