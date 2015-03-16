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
	
	private static int THREAD_NUM = 4;
	
	private ExecutorService executor;
	
	public Client(){
		// Set up number of available threads
		this.executor = Executors.newFixedThreadPool(THREAD_NUM); 
	}
	
	public void receiveChunkedFile(String doc_id, String doc_rev) throws IOException, InterruptedException, ExecutionException {
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		
		long flength = this.getFileLength(url, doc_id, doc_rev);
		System.out.println("flength: " + flength);
	
		int chunk_num = (int) Math.ceil(flength/(double)CH_SIZE);
		System.out.println("Chunks to receive: " + chunk_num);

		ArrayList<Integer> al = new ArrayList<Integer>();
		for (int i = 0; i < chunk_num; i++) al.add(i);
		
		this.chunkedDownload(url, "out_file.png", al, doc_id, flength);
		
		System.out.println("Download finished!");
	}
	
	private Future<Integer>[] receiveListedChunks(String filename, Integer[] chunks, String doc_id, URL url, long flength) throws IOException {
		System.out.println("Processing (in): " + chunks.length);
		
		Future<Integer>[] responses = new Future[chunks.length];
		int j = 0;
		for (int current_chunk : chunks){
			long start = current_chunk*CH_SIZE;
			long end = (current_chunk+1)*CH_SIZE;
			if (end > flength) end = flength;
			responses[j] = executor.submit(new AsyncGet(url, filename, doc_id, start, end));
			j++;
		}

		// Wait for completion of all tasks
		boolean done = true;
		for (int i = 0; i < chunks.length; i++) {
			done = done && responses[i].isDone();
			if ((i + 1 == chunks.length) && !done){
				i = -1;
				done = true;
			}
		}
		
		return responses;
	}
	
	private Future<String>[] sendListedChunks(String filename, int[] chunks, String doc_id, URL url) throws IOException {
		Future<String>[] responses = new Future[chunks.length];
		int j = 0;
		for (int current_chunk : chunks) {
			int start = current_chunk*CH_SIZE;
			int end = (current_chunk+1)*CH_SIZE;
			responses[j] = executor.submit(new AsyncPut(filename, url, doc_id, start, end));
			j++;
		}
		
		// Wait for completion of all tasks
		boolean done = true;
		for (int i = 0; i < chunks.length; i++) {
			//System.out.println(responses[i].isDone());
			done = done && responses[i].isDone();
			if ((i + 1 == chunks.length) && !done){
				i = -1;
				done = true;
			}
		}
		return responses;
	}
	
	public JSONObject sendChunkedFile(String filename, String doc_id, String rev_id) throws Exception {

		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		
		this.startFileTransfer(url, doc_id);
		
		File f = new File(filename);
		int chunk_num = (int) Math.ceil(f.length()/(double)CH_SIZE);
		
//		ArrayList<Integer> al = new ArrayList<Integer>();
		Stack<Integer> st = new Stack<Integer>();
		for (int i = 0; i < chunk_num; i++) st.push(i);
		
		this.chunkedUpload(url, st, filename, doc_id);

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
	
	// Download file chunks from the server until all have been received
	private void chunkedDownload(URL url, String filename, ArrayList<Integer> al, String doc_id, long flength) throws IOException {
		while (al.size() != 0) {
			Future<Integer>[] responses = this.receiveListedChunks(filename, al.toArray(new Integer[al.size()]), doc_id, url, flength);
			ArrayList<Integer> al_new = new ArrayList<Integer>();
			for (int i = 0; i < responses.length; i++) {
				try {
					int resp = responses[i].get();
					if (resp == 0){
						al_new.add(al.get(i));
						System.out.println("Checksum mismatch!");
					}
				} catch (ExecutionException|InterruptedException e) {
					al_new.add(al.get(i));
				}
			}
			al.clear(); // might be unnecessary
			al = al_new;
		}
	}
	
	// Send file chunks until all have been sent and acknowledged by server
	private void chunkedUpload(URL url, Stack<Integer> stack, String filename, String doc_id) throws IOException, InterruptedException {
		// TODO: add new threads on the go. (use .isDone())
		
		while (!stack.isEmpty()) {
//			System.out.println("Processing: " + stack.size());
			int chunks[] = new int[8*THREAD_NUM];
			for (int i = 0; i < 8*THREAD_NUM && !stack.isEmpty(); i++) chunks[i] = stack.pop();
			Future<String>[] responses = this.sendListedChunks(filename, chunks , doc_id, url);
			ArrayList<Integer> al_new = new ArrayList<Integer>();
			for (int i = 0; i < responses.length; i++) {
				try {
					// wait for completion
					String out = responses[i].get();
					// Check if the right chunk received
					if (!out.equals("Chunk received " + chunks[i]*CH_SIZE)){
						stack.push(chunks[i]);
					}
				} catch (ExecutionException e) {
					System.out.println("Execution exception");
					stack.push(chunks[i]);
				}
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