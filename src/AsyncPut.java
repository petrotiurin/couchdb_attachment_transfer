import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
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

import org.json.JSONObject;

import java.nio.channels.AsynchronousFileChannel;

public class AsyncPut implements Callable<String>{

	private URL url;
	private String doc_id;
	private String rev_id;
	private long start;
	private long end;
	private AsynchronousFileChannel fileChannel;
	
	public AsyncPut(AsynchronousFileChannel fileChannel, URL url, String doc_id, String rev_id, long start, long end) throws IOException {
		this.url = url;
		this.doc_id = doc_id;
		this.start = start;
		this.end = end;	
		this.rev_id = rev_id;
		this.fileChannel = AsynchronousFileChannel.open(
				Paths.get("file1.png"), StandardOpenOption.READ);;
	}
	
	@Override
	public String call() throws IOException, InterruptedException, ExecutionException, NoSuchAlgorithmException {
		try {
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();

			httpCon.setDoOutput(true);
			httpCon.setRequestMethod("PUT");
			httpCon.setRequestProperty("Content-Type", "application/octet-stream");
			httpCon.setRequestProperty("DocID", "" + doc_id);

			if (rev_id != null) {
				// Tell server to send file to the database.
				int timeout = 1000;
				httpCon.setConnectTimeout(timeout);
				httpCon.setReadTimeout(timeout);
				httpCon.setRequestProperty("RevID", "" + rev_id);
				System.out.println("Sending to server.");
			} else {
				// TIMEOUT
				int timeout = 1000;
				httpCon.setConnectTimeout(timeout);
				httpCon.setReadTimeout(timeout);
				// Fixed length streaming
//				httpCon.setFixedLengthStreamingMode(end-start);
				httpCon.setRequestProperty("Start", "" + start);
				if (end > fileChannel.size()) end = fileChannel.size();
				httpCon.setRequestProperty("End", "" + end);
				byte [] buffer = new byte[(int) (end-start)];
				// TODO: might be a better way to do this.
				Future<Integer> result = fileChannel.read(ByteBuffer.wrap(buffer), start);
				// Wait for it to finish
				int num_read = result.get();
				if (num_read != end-start) return "Not received.";
//				System.out.println(bytesToHex(Arrays.copyOfRange(buffer, 100, 120)));

				MessageDigest md = MessageDigest.getInstance("MD5");
				httpCon.setRequestProperty("MD5", bytesToHex(md.digest(buffer)));
				ByteArrayOutputStream out = (ByteArrayOutputStream) httpCon.getOutputStream();
				out.write(buffer);
				out.close();
			}
			InputStream response = httpCon.getInputStream();
			if (rev_id != null) System.out.println("Got the input stream");
			String resp_str = convertStreamToString(response);
			if (rev_id != null) System.out.println("Got the string");
			response.close();
			fileChannel.close();
			return resp_str;
		} catch (SocketTimeoutException e) {
			return "Not received.";
		}
	}
	
	// Read server response into string
	private static String convertStreamToString(InputStream in) throws IOException{
	    InputStreamReader is = new InputStreamReader(in);
		StringBuilder sb=new StringBuilder();
		BufferedReader br = new BufferedReader(is);
		String read = br.readLine();
		while(read != null) {
		    sb.append(read);
		    read = br.readLine();
		}
		return sb.toString();
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
