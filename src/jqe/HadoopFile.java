package jqe;
import java.io.FileReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class HadoopFile {
	//����һ��SequenceFile�ļ�λ��
	String uri = "hdfs://hadoop00:9000/mywork/test/sequencedemo.seq";
	Configuration conf = new Configuration();
	FileSystem fs = null;
	Path path = new Path(uri);
	@Test
	//����һ��������д�뵽SequenceFile�ļ��ķ���
	public void SequenceFileWriteDemo() throws Exception {
		fs = FileSystem.get(new URI(uri),conf);
		IntWritable key = new IntWritable();
		//����һ���ַ���������Ϊд������
		String[] data= {"one","two","three","four","five","six","seven","eight","nine","ten"};
		ArrayWritable aw = new ArrayWritable(data);
		Text value = new Text();
		Writer writer = null;
		try {
			//���Writer����
			//������writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),value.getClass());			
			SequenceFile.Writer.Option optionfile=Writer.file(path);
			SequenceFile.Writer.Option optionkey=Writer.keyClass(key.getClass());
			SequenceFile.Writer.Option optionvalue=Writer.valueClass(value.getClass());
			writer=SequenceFile.createWriter(conf, optionfile, optionkey, optionvalue);
			
			int i =1;   //���ڼ���
			for(Writable writable:aw.get()) {
				key.set(i);
				value.set(writable.toString());
				//׷�Ӽ�ֵ�Ե��ļ���
				writer.append(key, value);
				i++;
			}
		} finally {
			IOUtils.closeStream(writer);     //�ر���
		}
	}
	
	@Test
	//����һ����SequenceFile�ļ��ж�ȡ���ݵķ���
	public void SequenceFileReadDemo() throws Exception {
		fs = FileSystem.get(new URI(uri),conf);
		Reader reader = null;
		try {
			//����SequenceFile.Reader����
			//������reader = new SequenceFile.Reader(fs, path, conf);
			SequenceFile.Reader.Option optionfile=Reader.file(path);
			reader = new SequenceFile.Reader(conf, optionfile);
			
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key,value)) {     //������ȡ��¼
				//�滻�����ַ�
				String syscSeen = reader.syncSeen()?"*":" ";
				System.out.println(position+" "+syscSeen+" {"+key+","+value+"}");
				position = reader.getPosition();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	
	String uri1 = "hdfs://hadoop00:9000/mywork/test/mapfiledemo.map";
	Path path1 = new Path(uri1);

	@Test
	public void MapFileWriteDemo() throws Exception {
		//ָ������Ҫ��ȡ�ļ�
		FileReader fileReader = new FileReader("E:/data/hadoop.txt");
		int c;
		String line = "";
		while ((c=fileReader.read())!=-1) {
			line += (char)c;
		}
		//�з��ļ��ɾ���
		String[] sentence = line.split("\\.");
		IntWritable key = new IntWritable();
		Text value = new Text();
		org.apache.hadoop.io.MapFile.Writer writer = null;
		int p =1;
		try {
			writer = new MapFile.Writer(conf, fs, uri1, key.getClass(), value.getClass());
			for(String s:sentence) {
				key.set(p);
				value.set(s.trim());
				//��MapFile�ļ���׷�Ӽ�ֵ��
				writer.append(key, value);
				p++;
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	@Test
	public void MapFileReadDemo() throws Exception {
	fs = FileSystem.get(new URI(uri1),conf);
	org.apache.hadoop.io.MapFile.Reader reader =null;
	try {
		reader = new MapFile.Reader(fs, uri1, conf);
		WritableComparable key = (WritableComparable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			//��ӡ�ļ�����
			System.out.println(key+"   " +value);
		}
		System.out.println("��ȡkeyֵΪ10��ֵ");
		//��������ļ�����
		System.out.println(reader.get(new IntWritable(10), value));
		} finally {
			IOUtils.closeStream(reader);
		}
	}

}
