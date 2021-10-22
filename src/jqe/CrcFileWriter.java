package jqe;

import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
@SuppressWarnings("serial")
public class CrcFileWriter extends Exception{
	Configuration conf = new Configuration();
	@Test
	public void RawLocalWin() throws Exception {
		@SuppressWarnings("resource")
		//创建一个RawLocalFileSystem对象
		FileSystem fsRaw = new RawLocalFileSystem();
		fsRaw.initialize(URI.create("E://data/file/RawLocalFile.txt"), conf);
		//创建一个输出流，将结果输出到Windows系统本地，且无校验和
		OutputStream osWritter = fsRaw.create(new Path("E://data/file/RawLocalFile.txt"));
		osWritter.write(new String("这是一个RawLocalFileSystem测试程序").getBytes("utf-8"));
		osWritter.close();
	}
	
	@Test
	public void LocalWin() throws Exception {
		@SuppressWarnings("resource")
		//创建一个LocalFileSystem对象
		FileSystem fsRaw = new LocalFileSystem();
		fsRaw.initialize(URI.create("E://data/file/LocalFile.txt"), conf);
		//创建一个输出流，将结果输出到Windows系统本地
		OutputStream osWritter = fsRaw.create(new Path("E://data/file/LocalFile.txt"));
		osWritter.write(new String("这是一个LocalFileSystem测试程序").getBytes("utf-8"));
		osWritter.close();
	}
}