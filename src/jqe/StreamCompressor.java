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
			//��ȡ�ļ���
			String filename = file.getName();
			//��ȡ�ļ����ķ����ַ���
			String reversedFilename =new StringBuilder(filename).reverse().toString();
			SortedMap<String, CompressionCodec> subMap = codecs.headMap(reversedFilename);
			if (!subMap.isEmpty()) {
				String potentialSuffix = subMap.lastKey();
				if (reversedFilename.startsWith(potentialSuffix)) {
					//��ȡѹ������
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
		//����һ�����뷽��
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, conf);
		//ָ���������λ��
		OutputStream out1 = new FileOutputStream("E:/data/a.txt.gz");
		//ͨ�����������������Ӧ�������
		CompressionOutputStream out = codec.createOutputStream(out1);
		//�Խ�������������
		IOUtils.copyBytes(in1, out, 4096, false);
		out.finish();
	}

	@Test
	public void UnCompressor() throws IOException, URISyntaxException {
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, new Configuration());
		//����createInputStream()����
		CompressionInputStream in = codec.createInputStream(new FileInputStream("E:/data/a.txt.gz"));
		//�����ַ�����������
		BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
		//��ӡ���ݵ�����̨
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
			System.err.println("û���ҵ�ѹ����ѹ������" + uri);
			System.exit(1);
		}
		//��ѹ���ļ�����ѹ���ļ���ͬ��Ŀ¼��
		String outputUri =CompressionCodecFactory.removeSuffix(uri.toString(), codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			//�ر����������
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
		
	}
}
