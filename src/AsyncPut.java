import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;

public class AsyncPut extends AsyncTask{

	private FileChannel fileChannel;
	
	public AsyncPut(String filename, URL url, String doc_id, String doc_rev, long start, long end) throws IOException {
		super(filename, url, doc_id, doc_rev, start, end);
		this.fileChannel = FileChannel.open(Paths.get(filename), StandardOpenOption.READ);
	}
	
	@Override
	public String call() throws Exception{
//		throws IOException, InterruptedException, ExecutionException, NoSuchAlgorithmException
		try {
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();

			httpCon.setDoOutput(true);
			httpCon.setRequestMethod("PUT");
			httpCon.setRequestProperty("Content-Type", "application/octet-stream");
			httpCon.setRequestProperty("Chunked-Transfer", "true");
			
			// TIMEOUT
			int timeout = 500;
			httpCon.setConnectTimeout(timeout);
			httpCon.setReadTimeout(timeout);
			
			httpCon.setRequestProperty("Start", "" + start);
//			httpCon.setRequestProperty("If-Match", doc_rev);
			if (end > fileChannel.size()) end = fileChannel.size();
			httpCon.setRequestProperty("End", "" + end);
			byte [] buffer = new byte[(int) (end-start)];
			// Wait for it to finish
			int num_read = fileChannel.read(ByteBuffer.wrap(buffer), start);
			if (num_read != end-start) return "Not received.";

			MessageDigest md = MessageDigest.getInstance("MD5");
			httpCon.setRequestProperty("MD5", bytesToHex(md.digest(buffer)));
			ByteArrayOutputStream out = (ByteArrayOutputStream) httpCon.getOutputStream();
			out.write(buffer);
			out.close();

			if (httpCon.getResponseCode() != 201) {
				throw new Exception("Not received.");
			} else {
				InputStream response = httpCon.getInputStream();
				String resp_str = IOUtils.toString(response);
				//			resp_str = resp_str.trim();
				response.close();
				fileChannel.close();
				return resp_str;
			}
		} catch (Exception e) {
//			e.printStackTrace();
			fileChannel.close();
			throw new Exception(""+start);
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
