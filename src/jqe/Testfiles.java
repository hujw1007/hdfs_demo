package jqe;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
public class Testfiles {
	@Test
	public void createDir() throws Exception {
		Configuration conf = new Configuration();
		//ָ��HDFS��Ⱥ��ַ
		URI uri = new URI("hdfs://hadoop0:9000");
		//�����ļ�ϵͳ����
		FileSystem fs = FileSystem.get(uri, conf);
		//�����ļ�Ŀ¼
		Path dirs = new Path("/jqe/t1");
		try {
			//�ж��ļ�Ŀ¼�Ƿ���ڣ��������ھʹ������������򲻴���
			boolean re = fs.exists(dirs);
			if (!re) {
				fs.mkdirs(dirs);
				System.out.println("�ļ�Ŀ¼�Ѵ���");
			} else {
				System.out.println("�ļ�Ŀ¼�Ѵ���");
			}
		} catch (Exception e) {
			System.err.println("�����ļ�Ŀ¼����");
			e.printStackTrace();
		} finally {      //�ر�����
			fs.close();
		}
	}
	
	@Test
	public void putFiles() throws Exception {
		//�����ļ�ϵͳ����
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop0:9000"), new Configuration());
		//���屾���ļ�·��������·��λ�ã�
		Path win_local1 = new Path("D:/data1.txt");
		Path win_local2 = new Path("E:/data2.txt");
		//����HDFS�洢λ��
		Path dfs = new Path("/jqe/t1");
		//�ϴ���·���ļ�
		fs.copyFromLocalFile(win_local1, dfs);
		fs.copyFromLocalFile(win_local2, dfs);
		//�ļ��������
		FileStatus files[] = fs.listStatus(dfs);
		for (FileStatus file : files) {
				//��ӡ�ļ����·��
				System.out.println(file.getPath());
		}
		//�ر�����
		fs.close();
	}
	
	@Test
	public void getFiles() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop0:9000"), new Configuration());
		//����Ҫ���ļ����浽�ı���·��
		Path dispath = new Path("E:/data");
		//����Ҫ����HDFS�ļ��Ĵ洢λ��
		Path dfs = new Path("/input");
		//�ݹ��г���Ŀ¼�µ������ļ����������ļ��У�����ֵ��ʾ�Ƿ�ݹ飩
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(dfs, true);
		while (listFiles.hasNext()) {
			//�õ���һ���ļ���pop��listFiles
			LocatedFileStatus next = listFiles.next();
			//��ӡ�ļ�·��
			System.out.println(next.getPath());
			//�����ļ������ԡ�.txt��Ϊ��չ�����ļ����ص�����
			MyPathFilter myPathFilter = new MyPathFilter(".*\\.txt");
			if(!myPathFilter.accept(next.getPath())) {
				//����HDFS�ļ�������
				fs.copyToLocalFile(next.getPath(),dispath);
			System.out.println("���ص��ļ�Ϊ"+next.getPath().getName());
			}
		}
		//�ر�����
		fs.close();
	}
	/**
	 * ʵ��PathFilter�ӿڣ��Զ����ļ�������
	 * @author jqe
	 */
	class MyPathFilter implements PathFilter{
		String reg = null;
		public MyPathFilter(String reg) {
			this.reg=reg;
		}
		public boolean accept(Path path) {
			if(!path.toString().matches(reg)) {
				return true;
			}
			return false;
		}
	}
	
	@Test
	public void deleteFiles() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop0:9000"), new Configuration());
		//ɾ���ļ�����2������Ϊ�Ƿ�ݹ�ɾ���ļ��м��ļ����µ������ļ�
		fs.delete(new Path("/input/data1.txt"),true);
		//�ر�����
		fs.close();
	}
	
	@Test
	public void writeHDFS() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration());
		try {
			//�����ļ���
			Path dfs = new Path("/jqe/newfile.txt");
			//�������������
			FSDataOutputStream create = null;
			//���ļ������ڣ��ʹ����ļ���д�����ݣ����ļ����ڣ���׷������
			if(!fs.exists(dfs)) {
				//�����µ��ļ�����false����ʾ������ԭ�ļ�
				create = fs.create(dfs, false);
				//д������
				create.writeBytes("This is a HDFS file!\n");
				create.writeBytes("Welcome to Hadoop!\n");
				System.out.println("�µ�����д��ɹ�");
			} else {
				//�ļ����ڣ����ļ���׷���µ�����
				create = fs.append(dfs);
				create.writeBytes("Do you know HDFS?\n");
				System.out.println("�µ�����׷�ӳɹ�");
			}
		} catch (Exception e) {
			System.err.println("д�����ݴ���");
			e.printStackTrace();
		} finally {
			//�ر�����
			fs.close();
		}
	}
	
	@Test
	public void readHDFS() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), 
	new Configuration());
		//��ȡ��HDFS�ļ�·��
		Path path = new Path("hdfs://hadoop01:9000/jqe/newfile.txt");
		if (fs.exists(path)) {     //�ж��ļ��Ƿ����
			System.out.println("Exists!");
			try {
				//��ΪHadoop��ȡ��������
				FSDataInputStream is = fs.open(path);
				//����InputStreamReader����
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
				String line=null;     //�ݴ��ļ���������
				//�����ݷ��뻺����
				BufferedReader reader = new BufferedReader(inputStreamReader);
				//�ӻ�������ȡ����
				int i=0;    //�к�
				while((line=reader.readLine())!=null){
					i++;
					//��ӡÿ�е�����
					System.out.println("line"+i+" = "+line);
				}
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
	}
	
}
