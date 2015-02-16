import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Exception;
import java.io.File;
import java.lang.Math;
import java.net.URL;
import java.net.HttpURLConnection;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.json.JSONObject;
import org.apache.commons.io.IOUtils;

class Client {
	// Chunk size
	private static int CH_SIZE = 2048;
	
	private static String PATH = "ChunkServer/MainServlet"; //potato1
	private static String SERVER = "127.0.0.1";
	private static String PORT = "8080"; //5984

	private static String DB_NAME = "potato1";
	private static String DB_SERVER = "127.0.0.1";
	private static String DB_PORT = "5984";
	
	private ExecutorService executor;
	
	public Client(){
		this.executor = Executors.newFixedThreadPool(1); 
	}
	
	public void receiveChunkedFile(String doc_id, String doc_rev) throws IOException, InterruptedException, ExecutionException {
		AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
				Paths.get("out_file.png"), StandardOpenOption.READ,
		        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		
		int flength = this.getFileLength(doc_id, doc_rev);
		int current_chunk;
		int chunk_num = (int) Math.ceil(flength/(double)CH_SIZE); // TODO: safe cast from long
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		System.out.println("Chunks to receive: " + chunk_num);
		
		Future<Integer>[] responses = new Future[chunk_num];
		for (current_chunk = 0; current_chunk < chunk_num; current_chunk++){
			int start = current_chunk*CH_SIZE;
			int end = (current_chunk+1)*CH_SIZE;
			if (end > flength) end = flength;
			responses[current_chunk] = executor.submit(new AsyncGet(url, doc_id, start, end, fileChannel));
		}
		
		// Wait for completion of all tasks
		boolean done = true;
		for (int i = 0; i < chunk_num; i++) {
//			System.out.println(responses[i].isDone());
			done = done && responses[i].isDone();
			if ((i + 1 == chunk_num) && !done){
				i = -1;
				done = true;
			}
		}
		System.out.println("Download finished!");
	}
	
	public JSONObject sendChunkedFile(String filename, String docId, String revId) throws Exception {
		// Create new doc
		FileInputStream in = null;
		File f = new File(filename);
		int chunk_num = (int) Math.ceil(f.length()/(double)CH_SIZE);
		// Read document in chunks and upload them as attachments.
        try {
	        byte[] buffer = new byte[CH_SIZE];
	        in = new FileInputStream(f);
	        int current_chunk;
            for (current_chunk = 0; (current_chunk < chunk_num - 1) && (in.read(buffer) != -1) ; current_chunk++){
            	this.sendChunk(buffer, null, null);
	        }
            // Last chunk
            int last_chunk_size = (int) (f.length() % CH_SIZE);
            if (last_chunk_size == 0) last_chunk_size = CH_SIZE;
            buffer = new byte[last_chunk_size];
            in.read(buffer);
            JSONObject jo = this.sendChunk(buffer, docId, revId);
            return jo;
        } catch (Exception e) {
        	e.printStackTrace();
        	return null;
        } finally { 
             if ( in != null ) in.close();
             System.out.println("Upload finished!");
        }
	}
	
	// Sends a byte array as an individual attachment
	public JSONObject sendChunk(byte[] chunk, String docId, String revId) throws Exception{
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		if (docId != null && revId != null){
			httpCon.setRequestProperty("DocID", "" + docId);
			httpCon.setRequestProperty("RevID", "" + revId);
		}
		ByteArrayOutputStream out = (ByteArrayOutputStream) httpCon.getOutputStream();
		out.write(chunk);
		out.close();
		InputStream response = httpCon.getInputStream();
		String resp_str = convertStreamToString(response);
		response.close();
		if (!resp_str.equals("Chunk received")) {
			return new JSONObject(resp_str);
		} else {
			return null;
		}
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
			OutputStreamWriter out = new OutputStreamWriter(
			    httpCon.getOutputStream());
			out.write("{}");
			out.close();
			InputStream response = httpCon.getInputStream();
			String resp_string = convertStreamToString(response);
			response.close();
			JSONObject jo = new JSONObject(resp_string); 
			System.out.println("Doc created: " + jo.getString("id"));
			return jo;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
//
//	
	public FileOutputStream receiveChunk(String doc_id, String rev_id, FileOutputStream fs, int start, int end) throws IOException{
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("Start", "" + start);
		httpCon.setRequestProperty("End", "" + end);
		httpCon.setRequestProperty("DocId", "" + doc_id);
		InputStream response = httpCon.getInputStream();
		if (fs == null){
			fs = new FileOutputStream("out_file.png");
		}
		IOUtils.copy(response,fs);
		response.close();
		return fs;
	}
	
	private int getFileLength(String doc_id, String rev_id) throws IOException {
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH);
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("DocId", "" + doc_id);
		httpCon.setRequestProperty("RevId", "" + rev_id);
		InputStream response = httpCon.getInputStream();
		String length = convertStreamToString(response);
		return Integer.parseInt(length);
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