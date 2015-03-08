import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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
	private long start;
	private long end;
	private AsynchronousFileChannel fileChannel;

	public AsyncGet(URL url, String doc_id, long start, long end, AsynchronousFileChannel afc) {
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
		
		int chunk_size = (int) (end - start);
		InputStream response = httpCon.getInputStream();
		byte [] buffer = new byte[32 + chunk_size];
		byte [] file_chunk = new byte[chunk_size];
		byte [] md5 = new byte[32];
		response.read(buffer);
		System.arraycopy(buffer, chunk_size, md5, 0, 32);
		System.arraycopy(buffer, 0, file_chunk, 0, chunk_size);

		MessageDigest md = MessageDigest.getInstance("MD5");
		if (Arrays.equals(bytesToHex(md.digest(file_chunk)).getBytes(), md5)) {
			
			System.out.println(bytesToHex(Arrays.copyOfRange(file_chunk, 100, 120)));
			
			Future<Integer> result = fileChannel.write(ByteBuffer.wrap(file_chunk), start);
			response.close();
			int resp = result.get();
//			System.out.println("Wrote " + resp);
			return resp;
		} else {
			System.out.println("checksum mismatch");
			// Return failure
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
