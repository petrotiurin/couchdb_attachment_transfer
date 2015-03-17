import java.io.IOException;
import java.lang.Exception;
import java.io.File;
import java.lang.Math;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.HttpURLConnection;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.OutputStreamWriter;

import org.json.JSONException;
import org.json.JSONObject;
import org.apache.commons.io.IOUtils;

class Client {
	// Chunk size
	private static int CH_SIZE = 2048;
	
	private static String PATH = "ChunkServer/MainServlet";
	private static String SERVER = "127.0.0.1";
	private static String PORT = "8080"; 
	
	private static String DB_NAME = "potato1";
	private static String DB_SERVER = "127.0.0.1";
	private static String DB_PORT = "5984";
	
	private static int THREAD_NUM = 20;
	
	private ExecutorService executor;
	
	public Client(){
		// Set up number of available threads
		this.executor = Executors.newFixedThreadPool(THREAD_NUM); 
	}
	
	public void receiveChunkedFile(String doc_id, String doc_rev) throws IOException, InterruptedException, ExecutionException, ClassNotFoundException {
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		
		long flength = this.getFileLength(url, doc_id, doc_rev);
		System.out.println("flength: " + flength);
	
		int chunk_num = (int) Math.ceil(flength/(double)CH_SIZE);
		System.out.println("Chunks to receive: " + chunk_num);

		Stack<Integer> st = new Stack<Integer>();
		for (int i = 0; i < chunk_num; i++) st.push(i);
		
		this.chunkedOperation(url, st, "out_file.png", doc_id, flength, AsyncGet.class.getName());
		
		System.out.println("Download finished!");
	}
	
	public JSONObject sendChunkedFile(String filename, String doc_id, String rev_id) throws Exception {

		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		
		this.startFileTransfer(url, doc_id);
		
		File f = new File(filename);
		int chunk_num = (int) Math.ceil(f.length()/(double)CH_SIZE);
		System.out.println("Chunks to send: " + chunk_num);
		
		Stack<Integer> st = new Stack<Integer>();
		for (int i = 0; i < chunk_num; i++) st.push(i);
		
		this.chunkedOperation(url, st, filename, doc_id, f.length(), AsyncPut.class.getName());

		System.out.println("Chunks sent. Asking server to update the doc.");
		// Tell server to send the file.
		JSONObject jo = this.finaliseUpload(url,doc_id, rev_id);
		System.out.println("Upload finished!");
		return jo;
	}
	
	// Creates a doc with random id
	public JSONObject createNewDoc() {
		try {
			String doc_id = "a" + UUID.randomUUID().toString();
			URL url = new URL("http://" + DB_SERVER + ":" + DB_PORT + "/"
							  + DB_NAME + "/" + doc_id);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
			httpCon.setDoOutput(true);
			httpCon.setRequestMethod("PUT");
			httpCon.setRequestProperty("Content-Type", "application/json");
			OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
			out.write("{}");
			out.close();
			InputStream response = httpCon.getInputStream();
			String resp_string = IOUtils.toString(response);
			resp_string = resp_string.trim();
			response.close();
			JSONObject jo = new JSONObject(resp_string);
			System.out.println(jo.getString("id"));
			return jo;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// Send/receive file chunks until all have been sent/received and acknowledged
	private void chunkedOperation(URL url, Stack<Integer> stack, String filename, String doc_id, long flength, String className) throws IOException, InterruptedException, ClassNotFoundException {
		
		int nsimul_tasks = 2*THREAD_NUM;
		
		Future<Object>[] responses = new Future[nsimul_tasks];
		// Keep track of processed chunks
		int chunks_in_process[] = new int[nsimul_tasks];
		// Initial send threads
		for (int i = 0; i < nsimul_tasks && !stack.isEmpty(); i++) {
			int chunk = stack.pop();
			long start = chunk*CH_SIZE;
			long end = (chunk+1)*CH_SIZE;
			if (end > flength) end = flength;
			AsyncTask at;
			if (AsyncGet.class.equals(Class.forName(className))) at = new AsyncGet(filename, url, doc_id, start, end);
			else at = new AsyncPut(filename, url, doc_id, start, end);
			responses[i] = executor.submit(at);
			chunks_in_process[i] = chunk;
		}
		
		// Replace with new if any of the threads are finished
		boolean done = false;
		while (!done) {
			done = true;
			for (int i = 0; i < chunks_in_process.length; i++) {
				// Check if any threads are finished
				if (responses[i] != null && responses[i].isDone()) {
					int chunk;
					try {
						String resp = (String) responses[i].get();
						if (resp.equals("Not received.") || resp.equals("0")) {
							// If task failed - retry
							chunk = chunks_in_process[i];
						} else {
							// If task succeeded
							if (stack.isEmpty()){
								responses[i] = null;
								continue;
							}
							chunk = stack.pop();
						}
					} catch (ExecutionException|InterruptedException e) {
						// Exception is a failure, retry
						chunk = chunks_in_process[i];
					}
					long start = chunk*CH_SIZE;
					long end = (chunk+1)*CH_SIZE;
					AsyncTask at;
					if (AsyncGet.class.equals(Class.forName(className))) at = new AsyncGet(filename, url, doc_id, start, end);
					else at = new AsyncPut(filename, url, doc_id, start, end);
					responses[i] = executor.submit(at);
					chunks_in_process[i] = chunk;
				}
				if (responses[i] != null) done = false;
			}
		}
	}
	
	// Tell server to send file to the database
	private JSONObject finaliseUpload(URL url, String doc_id, String rev_id) throws IOException {
		while (true) {
			try {
				int timeout = 1000;
				HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				httpCon.setDoOutput(true);
				httpCon.setRequestMethod("PUT");
				httpCon.setRequestProperty("Content-Type", "application/octet-stream");
				httpCon.setRequestProperty("DocID", "" + doc_id);
				httpCon.setConnectTimeout(timeout);
				httpCon.setReadTimeout(timeout);
				httpCon.setRequestProperty("RevID", "" + rev_id);
				InputStream response = httpCon.getInputStream();
				String resp_str = IOUtils.toString(response);
				resp_str = resp_str.trim();
				response.close();
				return new JSONObject(resp_str);
			} catch (SocketTimeoutException|JSONException e) {
				// Just retry
			}
		}
	}
	
	// Initiate the file transfer with the server
	private void startFileTransfer(URL url, String doc_id) throws IOException, SocketTimeoutException {
		while (true){
			try {
				HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				int timeout = 500;
				httpCon.setConnectTimeout(timeout);
				httpCon.setReadTimeout(timeout);
				httpCon.setDoOutput(true);
				httpCon.setRequestMethod("PUT");
				httpCon.setRequestProperty("Content-Type", "application/octet-stream");
				httpCon.setRequestProperty("DocId", "" + doc_id);
				int resp = httpCon.getResponseCode();
				if (resp == 200) return;
			} catch (SocketTimeoutException e) {
				// Just retry
			}
		}
	}
	
	// Query the server for file length
	private long getFileLength(URL url_2, String doc_id, String rev_id) {
		// Try until succeeded
		while (true) {
			try {
				URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
				HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				int timeout = 1000;
				httpCon.setConnectTimeout(timeout);
				httpCon.setReadTimeout(timeout);
				httpCon.setDoOutput(true);
				httpCon.setRequestMethod("GET");
				httpCon.setRequestProperty("Content-Type", "application/octet-stream");
				httpCon.setRequestProperty("DocId", "" + doc_id);
				httpCon.setRequestProperty("RevId", "" + rev_id);
				InputStream response = httpCon.getInputStream();
				String length = IOUtils.toString(response);
				length = length.trim();
				return Long.parseLong(length);
			} catch (IOException e) {
				// Just retry.
			}
		}
	}
}