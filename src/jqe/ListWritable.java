package jqe;

//public class ListWritable {

//}

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
public class ListWritable<E extends Writable> implements Writable{
	private List<E> instance;
	//空的构造函数
	public ListWritable() {
		super();
	}
	public ListWritable(List<E> instance) {
		this.instance = instance;
	}
	public void push(E e) {
		if (instance == null) {
			instance = new ArrayList<E>();
		}
		instance.add(e);
	}
	public int size() {
		return instance.size();
	}
	//覆盖toString方法
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(E e : instance){
			if(e instanceof ArrayWritable) {  //判断是否为ArrayWritable对象
				ArrayWritable aw = (ArrayWritable)e;
				sb.append("[");
				String tmp = "";
				for(Writable wa : aw.get()) {
					tmp+=wa.toString()+", ";
				}
				sb.append(tmp.substring(0, tmp.length()-2));
				sb.append("]");
				sb.append(",");
			}
			else {
				sb.append(e);
				sb.append(",");
			}
			
		}
		String result = (String) sb.subSequence(0, sb.length() - 1);
		return result + "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
	}
	@Override
	public void readFields(DataInput in) throws IOException {
	}
	public static void main(String[] args) {
		//测试两个ArrayWritable加入一个List
		ListWritable<ArrayWritable> lw = new ListWritable<ArrayWritable>();
	    ArrayWritable aw = new ArrayWritable(new String[] {"19","92","08","28"});
	    ArrayWritable aw1 = new ArrayWritable(new String[] {"19","92","08","28"});
	    lw.push(aw);
	    lw.push(aw1);
		System.out.println(lw.toString());
		//测试整数
		ListWritable<IntWritable> lw1 = new ListWritable<IntWritable>();
		IntWritable iWritable = new IntWritable();
		iWritable.set(222);
		lw1.push(iWritable);
		lw1.push(new IntWritable(555));
		System.out.println(lw1.toString());
		//测试文本
		ListWritable<Text> lw2 = new ListWritable<Text>();
		Text text = new Text("This is a test!");
		lw2.push(text);
		System.out.println(lw2.toString());
	}
}
