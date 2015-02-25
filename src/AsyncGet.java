import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


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
	public Integer call() throws IOException, ExecutionException, InterruptedException, SocketTimeoutException, NoSuchAlgorithmException {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		
		// TIMEOUT
		int timeout = 500;
		httpCon.setConnectTimeout(timeout);
		httpCon.setReadTimeout(timeout);
		
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("Start", "" + start);
		httpCon.setRequestProperty("End", "" + end);
		httpCon.setRequestProperty("DocId", "" + doc_id);
		InputStream response = httpCon.getInputStream();
		byte [] buffer = new byte[16 + end-start];
		byte [] md5 = new byte[16];
		response.read(buffer);
		System.arraycopy(buffer, end - start, md5, 0, 16);
		MessageDigest md = MessageDigest.getInstance("MD5");
		System.out.println(bytesToHex(md5));
		System.out.println(bytesToHex(md.digest(buffer)));
		if (Arrays.equals(md.digest(buffer), md5)) {
			Future<Integer> result = fileChannel.write(ByteBuffer.wrap(buffer,0, end - start), start);
			response.close();
			return result.get();
		} else {
			return 0;
		}
	}
	
	public static String bytesToHex(byte[] bytes) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}

}
