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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.json.JSONObject;


public class AsyncPut implements Callable<String>{

	private URL url;
	private String doc_id;
	private String rev_id;
	private int start;
	private int end;
	private AsynchronousFileChannel fileChannel;
	
	public AsyncPut(AsynchronousFileChannel fileChannel, URL url, String doc_id, String rev_id, int start, int end) {
		this.url = url;
		this.doc_id = doc_id;
		this.start = start;
		this.end = end;	
		this.rev_id = rev_id;
		this.fileChannel = fileChannel;
	}
	
	@Override
	public String call() throws IOException, InterruptedException, ExecutionException, SocketTimeoutException {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		
		// TIMEOUT
		int timeout = 3000;
		httpCon.setConnectTimeout(timeout);
		httpCon.setReadTimeout(timeout);
		
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("DocID", "" + doc_id);
		
		if (rev_id != null) {
			httpCon.setRequestProperty("RevID", "" + rev_id);
		} else {
			httpCon.setRequestProperty("Start", "" + start);
			httpCon.setRequestProperty("End", "" + end);
			ByteArrayOutputStream out = (ByteArrayOutputStream) httpCon.getOutputStream();
			byte [] buffer = new byte[end-start];
			// TODO: might be a better way to do this.
			Future<Integer> result = fileChannel.read(ByteBuffer.wrap(buffer), start);
			// Wait for it to finish
			result.get();
			out.write(buffer);
			out.close();
		}
		InputStream response = httpCon.getInputStream();
		String resp_str = convertStreamToString(response);
		response.close();
		return resp_str;
	}
	
	// Read server response into string
	private static String convertStreamToString(InputStream in) throws IOException{
	    InputStreamReader is = new InputStreamReader(in);
		StringBuilder sb=new StringBuilder();
		BufferedReader br = new BufferedReader(is);
		String read = br.readLine();
		while(read != null) {
		    sb.append(read);
		    read =br.readLine();
		}
		return sb.toString();
	}

}
