package cn.tao.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupComparator extends WritableComparator {

	public DateTemperatureGroupComparator(){
		super(DateTemperaturePair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateTemperaturePair d1=(DateTemperaturePair)a;
		DateTemperaturePair d2=(DateTemperaturePair)b;
		return d1.getYearMonth().compareTo(d2.getYearMonth());
	}
}
