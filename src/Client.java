import java.io.IOException;
import java.lang.Exception;
import java.io.File;
import java.lang.Math;
import java.net.MalformedURLException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.OutputStreamWriter;

import org.json.JSONException;
import org.json.JSONObject;
import org.apache.commons.io.IOUtils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class Client {
	// Chunk size
	public static int CH_SIZE;
	
//	private static String PATH = "ChunkServer/MainServlet";
//	private static String SERVER = "127.0.0.1";
//	private static String PORT = "8080"; 
	
	private static String PATH = "potato1";
	private static String SERVER = "127.0.0.1";
	private static String PORT = "5984"; 
	
	private static String DB_NAME = "potato1";
	private static String DB_SERVER = "127.0.0.1";
	private static String DB_PORT = "5984";
	
	private static int THREAD_NUM = 4;
	
	private final Stack<Integer> chunkStack = new Stack<Integer>();
	private final int chunks_in_process[] = new int[THREAD_NUM*2];
	private final String filename;
	private final URL url;
	private final String doc_id, rev_id;
	
	
	private ListeningExecutorService executor;
	
	public Client(String filename, String doc_id, String doc_rev, int chunk_size) throws MalformedURLException{
		// Set up number of available threads
		this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_NUM)); 
		this.url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH + "/" + doc_id + "/" + filename);
		this.doc_id = doc_id;
		this.rev_id = doc_rev;
		this.filename = filename;
		this.CH_SIZE = chunk_size;
	}
	
	public void receiveChunkedFile(String doc_id, String doc_rev) throws IOException, InterruptedException, ExecutionException, ClassNotFoundException {
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		
		long flength = this.getFileLength(url, doc_id, doc_rev);
		System.out.println("flength: " + flength);
	
		int chunk_num = (int) Math.ceil(flength/(double)CH_SIZE);
		System.out.println("Chunks to receive: " + chunk_num);

//		chunkStack = new Stack<Integer>();
		for (int i = 0; i < chunk_num; i++) chunkStack.push(i);
		
		this.chunkedOperation(url, chunkStack, "out_file.png", doc_id, doc_rev, flength, AsyncGet.class.getName());
		
		System.out.println("Download finished!");
	}
	
	public JSONObject sendChunkedFile(String filename, String doc_id, String rev_id) throws Exception {

		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH + "/" + doc_id + "/" + filename);
		
//		this.startFileTransfer(url, doc_id);
		
		File f = new File(filename);
		int chunk_num = (int) Math.ceil(f.length()/(double)CH_SIZE);
		System.out.println("Chunks to send: " + chunk_num);
		
//		chunkStack = new Stack<Integer>();
		for (int i = 0; i < chunk_num; i++) chunkStack.push(i);
		
		this.chunkedOperation(url, chunkStack, filename, doc_id, rev_id, f.length(), AsyncPut.class.getName());

		System.out.println("Chunks sent. Asking server to update the doc.");
		// Tell server to send the file.
		JSONObject jo = this.finaliseUpload(url, doc_id, rev_id);
		System.out.println("Upload finished!" + rev_id);
		return null;
//		return jo;
	}
	
	// Creates a doc with random id
	public static JSONObject createNewDoc() {
		while (true){
			try {
				String doc_id = "a" + UUID.randomUUID().toString();
				URL url = new URL("http://" + DB_SERVER + ":" + DB_PORT + "/"
						+ DB_NAME + "/" + doc_id);
				HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
				httpCon.setDoOutput(true);
				httpCon.setConnectTimeout(300);
				httpCon.setReadTimeout(300);
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
			} catch (Exception e) {
//				e.printStackTrace();
//				return null;
			}
		}
	}
	
	private ListenableFuture<Object> createNewTask(String className) throws IOException, ClassNotFoundException {
		int chunk = chunkStack.pop();
//		System.out.println(chunk);
		long start = chunk*CH_SIZE;
		long end = (chunk+1)*CH_SIZE;
		AsyncTask at;
		if (AsyncGet.class.equals(Class.forName(className))) at = new AsyncGet(filename, url, doc_id, rev_id, start, end);
		else at = new AsyncPut(filename, url, doc_id, rev_id, start, end);
		return executor.submit(at);
	}
	
	private void addCallback(ListenableFuture<Object> lf, final CountDownLatch cdl) {
		Futures.addCallback(lf, new FutureCallback<Object>() {
			public void onSuccess(Object explosion) {
				if (!chunkStack.isEmpty()){
					try {
						ListenableFuture<Object> new_lf = createNewTask(this.getClass().getName());
						addCallback(new_lf, cdl);
					} catch (ClassNotFoundException|IOException e) {
						e.printStackTrace();
					}
				} else {
					cdl.countDown();
				}
			}
			public void onFailure(Throwable thrown) {
				Long start = Long.parseLong(thrown.getMessage());
				chunkStack.push((int) (start/Client.CH_SIZE));
//				System.out.println(start);
				try {
					ListenableFuture<Object> new_lf = createNewTask(this.getClass().getName());
					addCallback(new_lf, cdl);
				} catch (ClassNotFoundException|IOException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	// Send/receive file chunks until all have been sent/received and acknowledged
	private void chunkedOperation(URL url, Stack<Integer> stack, String filename, String doc_id, String rev_id, long flength, String className) throws IOException, InterruptedException, ClassNotFoundException {
		
		int nsimul_tasks = THREAD_NUM;
		
		CountDownLatch cdl = new CountDownLatch(nsimul_tasks);
		
		// Initial send threads
		for (int i = 0; i < nsimul_tasks && !stack.isEmpty(); i++) {
			ListenableFuture<Object> response = createNewTask(className);
			addCallback(response, cdl);
		}
		
		cdl.await();
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
				httpCon.setConnectTimeout(timeout);
//				httpCon.setReadTimeout(timeout);
				httpCon.setRequestProperty("If-Match", rev_id);
				httpCon.setRequestProperty("Chunked-Transfer", "final");
				InputStream response = httpCon.getInputStream();
				String resp_str = IOUtils.toString(response);
				resp_str = resp_str.trim();
				response.close();
				return new JSONObject(resp_str);
			} catch (SocketTimeoutException|JSONException e) {
				// Just retry
				e.printStackTrace();
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