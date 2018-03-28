package cn.tao.multipleinputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 *@author  Tao wenjun
 *封装信息的类
 */

public class InfoBean implements Writable{
	private String account;
	private double income;
	private double expense;
	
	public InfoBean(){
		
	}
	
	public InfoBean(String account,double income,double expense){
		this.account=account;
		this.income=income;
		this.expense=expense;
	}
	
	/**
	 * @return the account
	 */
	public String getAccount() {
		return account;
	}
	/**
	 * @param account the account to set
	 */
	public void setAccount(String account) {
		this.account = account;
	}
	/**
	 * @return the income
	 */
	public double getIncome() {
		return income;
	}
	/**
	 * @param income the income to set
	 */
	public void setIncome(double income) {
		this.income = income;
	}
	/**
	 * @return the expense
	 */
	public double getExpense() {
		return expense;
	}
	/**
	 * @param expense the expense to set
	 */
	public void setExpense(double expense) {
		this.expense = expense;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeUTF(String.valueOf(income));
		out.writeUTF(String.valueOf(expense));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		account=in.readUTF();
		income=Double.parseDouble(in.readUTF());
		expense=Double.parseDouble(in.readUTF());
	}
	
	@Override
	public String toString(){
		return this.account+"\t"+this.income+"\t"+this.expense;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}
	
}
