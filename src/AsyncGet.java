import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Callable;


public class AsyncGet implements Callable<Integer>{
	
	private URL url;
	private String doc_id;
	private int start;
	private int end;
	private AsynchronousFileChannel fileChannel;

	public AsyncGet(URL url, String doc_id, int start, int end, AsynchronousFileChannel afc) {
		this.url = url;
		this.doc_id = doc_id;
		this.start = start;
		this.end = end;
		this.fileChannel = afc;
	}
	
	@Override
	public Integer call() throws Exception {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("Start", "" + start);
		httpCon.setRequestProperty("End", "" + end);
		httpCon.setRequestProperty("DocId", "" + doc_id);
		InputStream response = httpCon.getInputStream();
		byte [] buffer = new byte[end-start];
		response.read(buffer);
		fileChannel.write(ByteBuffer.wrap(buffer), start);
		response.close();
		// meaningful response?
		return 1;
	}

}
