package jqe;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class StreamCompressor {
	private SortedMap<String, CompressionCodec> codecs = null;
	public CompressionCodec getCodec(Path file) {
		CompressionCodec result = null;
		if (codecs != null) {
			//获取文件名
			String filename = file.getName();
			//获取文件名的反序字符串
			String reversedFilename =new StringBuilder(filename).reverse().toString();
			SortedMap<String, CompressionCodec> subMap = codecs.headMap(reversedFilename);
			if (!subMap.isEmpty()) {
				String potentialSuffix = subMap.lastKey();
				if (reversedFilename.startsWith(potentialSuffix)) {
					//获取压缩方法
					result = codecs.get(potentialSuffix);
				}
			}
		}
		return result;
	}
	
	@Test
	public void Compressor() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop00:9000"), conf);
		FSDataInputStream in1 = fs.open(new Path("/mywork/a.txt"));
		//传入一个解码方法
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, conf);
		//指定本地输出位置
		OutputStream out1 = new FileOutputStream("E:/data/a.txt.gz");
		//通过编码解码器创建对应的输出流
		CompressionOutputStream out = codec.createOutputStream(out1);
		//对接流，传输数据
		IOUtils.copyBytes(in1, out, 4096, false);
		out.finish();
	}

	@Test
	public void UnCompressor() throws IOException, URISyntaxException {
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, new Configuration());
		//调用createInputStream()方法
		CompressionInputStream in = codec.createInputStream(new FileInputStream("E:/data/a.txt.gz"));
		//定义字符缓冲输入流
		BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
		//打印数据到控制台
		bReader.lines().iterator().forEachRemaining(x->System.out.println(x));
	}
	
	@Test
	public void FileDecompressor() throws Exception{
		Configuration conf = new Configuration();
		String location ="hdfs://hadoop00:9000/mywork/test/a.txt.gz";
		URI uri = new URI(location);
		FileSystem fs = FileSystem.get(uri, conf);
		Path inputPath = new Path(location);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("没有找到压缩解压缩方法" + uri);
			System.exit(1);
		}
		//解压缩文件到与压缩文件相同的目录下
		String outputUri =CompressionCodecFactory.removeSuffix(uri.toString(), codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			//关闭输入输出流
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
		
	}
}
