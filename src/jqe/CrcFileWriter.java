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
		//����һ��RawLocalFileSystem����
		FileSystem fsRaw = new RawLocalFileSystem();
		fsRaw.initialize(URI.create("E://data/file/RawLocalFile.txt"), conf);
		//����һ�������������������Windowsϵͳ���أ�����У���
		OutputStream osWritter = fsRaw.create(new Path("E://data/file/RawLocalFile.txt"));
		osWritter.write(new String("����һ��RawLocalFileSystem���Գ���").getBytes("utf-8"));
		osWritter.close();
	}
	
	@Test
	public void LocalWin() throws Exception {
		@SuppressWarnings("resource")
		//����һ��LocalFileSystem����
		FileSystem fsRaw = new LocalFileSystem();
		fsRaw.initialize(URI.create("E://data/file/LocalFile.txt"), conf);
		//����һ�������������������Windowsϵͳ����
		OutputStream osWritter = fsRaw.create(new Path("E://data/file/LocalFile.txt"));
		osWritter.write(new String("����һ��LocalFileSystem���Գ���").getBytes("utf-8"));
		osWritter.close();
	}
}