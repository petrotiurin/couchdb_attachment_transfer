import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class AsyncGet extends AsyncTask {
	
	private FileChannel fileChannel;

	public AsyncGet(String filename, URL url, String doc_id, String rev_id, long start, long end) throws IOException {
		super(filename, url, doc_id, rev_id, start, end);		
		this.fileChannel = FileChannel.open(Paths.get(filename), 
				StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);
	}
	
	@Override
	public String call() throws IOException, ExecutionException, InterruptedException, SocketTimeoutException, NoSuchAlgorithmException {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		
		// TIMEOUT
		int timeout = 1000;
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
		int read = response.read(buffer);
		response.close();
		if (read < chunk_size) {
			byte[] buffer_old = buffer;
			buffer = new byte[read];
			System.arraycopy(buffer_old, 0, buffer, 0, read);
			chunk_size = read - 32;
		}
		byte [] file_chunk = new byte[chunk_size];
		byte [] md5 = new byte[32];
		System.arraycopy(buffer, chunk_size, md5, 0, 32);
		System.arraycopy(buffer, 0, file_chunk, 0, chunk_size);

		MessageDigest md = MessageDigest.getInstance("MD5");
		if (Arrays.equals(bytesToHex(md.digest(file_chunk)).getBytes(), md5)) {
			int resp  = fileChannel.write(ByteBuffer.wrap(file_chunk), start);
			fileChannel.close();
			return String.valueOf(resp);
		} else {
			System.out.println("MD5 mismatch!");
			// Return failure
			fileChannel.close();
			return "0";
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
