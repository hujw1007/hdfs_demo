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
		//指定HDFS集群地址
		URI uri = new URI("hdfs://hadoop0:9000");
		//创建文件系统对象
		FileSystem fs = FileSystem.get(uri, conf);
		//创建文件目录
		Path dirs = new Path("/jqe/t1");
		try {
			//判断文件目录是否存在，若不存在就创建，若存在则不创建
			boolean re = fs.exists(dirs);
			if (!re) {
				fs.mkdirs(dirs);
				System.out.println("文件目录已创建");
			} else {
				System.out.println("文件目录已存在");
			}
		} catch (Exception e) {
			System.err.println("创建文件目录错误");
			e.printStackTrace();
		} finally {      //关闭连接
			fs.close();
		}
	}
	
	@Test
	public void putFiles() throws Exception {
		//创建文件系统对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop0:9000"), new Configuration());
		//定义本地文件路径（两个路径位置）
		Path win_local1 = new Path("D:/data1.txt");
		Path win_local2 = new Path("E:/data2.txt");
		//定义HDFS存储位置
		Path dfs = new Path("/jqe/t1");
		//上传多路径文件
		fs.copyFromLocalFile(win_local1, dfs);
		fs.copyFromLocalFile(win_local2, dfs);
		//文件存放数组
		FileStatus files[] = fs.listStatus(dfs);
		for (FileStatus file : files) {
				//打印文件存放路径
				System.out.println(file.getPath());
		}
		//关闭连接
		fs.close();
	}
	
	@Test
	public void getFiles() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop0:9000"), new Configuration());
		//定义要将文件保存到的本地路径
		Path dispath = new Path("E:/data");
		//定义要下载HDFS文件的存储位置
		Path dfs = new Path("/input");
		//递归列出该目录下的所有文件（不包括文件夹，布尔值表示是否递归）
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(dfs, true);
		while (listFiles.hasNext()) {
			//得到下一个文件并pop出listFiles
			LocatedFileStatus next = listFiles.next();
			//打印文件路径
			System.out.println(next.getPath());
			//过滤文件，将以“.txt”为扩展名的文件下载到本地
			MyPathFilter myPathFilter = new MyPathFilter(".*\\.txt");
			if(!myPathFilter.accept(next.getPath())) {
				//保存HDFS文件到本地
				fs.copyToLocalFile(next.getPath(),dispath);
			System.out.println("下载的文件为"+next.getPath().getName());
			}
		}
		//关闭连接
		fs.close();
	}
	/**
	 * 实现PathFilter接口，自定义文件过滤类
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
		//删除文件，第2个参数为是否递归删除文件夹及文件夹下的数据文件
		fs.delete(new Path("/input/data1.txt"),true);
		//关闭连接
		fs.close();
	}
	
	@Test
	public void writeHDFS() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration());
		try {
			//定义文件名
			Path dfs = new Path("/jqe/newfile.txt");
			//创建输出流对象
			FSDataOutputStream create = null;
			//若文件不存在，就创建文件并写入数据；若文件存在，就追加数据
			if(!fs.exists(dfs)) {
				//创建新的文件，“false”表示不覆盖原文件
				create = fs.create(dfs, false);
				//写入数据
				create.writeBytes("This is a HDFS file!\n");
				create.writeBytes("Welcome to Hadoop!\n");
				System.out.println("新的数据写入成功");
			} else {
				//文件存在，在文件中追加新的数据
				create = fs.append(dfs);
				create.writeBytes("Do you know HDFS?\n");
				System.out.println("新的数据追加成功");
			}
		} catch (Exception e) {
			System.err.println("写入数据错误");
			e.printStackTrace();
		} finally {
			//关闭连接
			fs.close();
		}
	}
	
	@Test
	public void readHDFS() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), 
	new Configuration());
		//读取的HDFS文件路径
		Path path = new Path("hdfs://hadoop01:9000/jqe/newfile.txt");
		if (fs.exists(path)) {     //判断文件是否存在
			System.out.println("Exists!");
			try {
				//此为Hadoop读取数据类型
				FSDataInputStream is = fs.open(path);
				//创建InputStreamReader对象
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
				String line=null;     //暂存文件的行数据
				//将数据放入缓冲区
				BufferedReader reader = new BufferedReader(inputStreamReader);
				//从缓冲区读取数据
				int i=0;    //行号
				while((line=reader.readLine())!=null){
					i++;
					//打印每行的数据
					System.out.println("line"+i+" = "+line);
				}
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
	}
	
}
