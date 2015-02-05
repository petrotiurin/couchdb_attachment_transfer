import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Exception;
import java.io.File;
import java.lang.Math;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.InputStream;
import java.util.UUID;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.json.JSONObject;
import org.apache.commons.io.IOUtils;

class Client {
	// Chunk size
	private static int CH_SIZE = 2048;
	private static String DB_NAME = "potato1";
	private static String URL = "127.0.0.1";
	private static String PORT = "5984";

	public void receiveChunkedFile(String doc_id, String doc_rev) throws Exception {
		FileOutputStream fs = null;
		int c_num = getChunkNum(doc_id);
		for (int i = 0; i < c_num; i++) {
			fs = this.receiveChunk(doc_id, doc_rev, i, fs);
		}
		fs.close();
		System.out.println("Download finished!");
	}
	
	public JSONObject sendChunkedFile(String filename) throws Exception {
		// Create new doc
		FileInputStream in = null;
		File f = new File(filename);
		int chunk_num = (int) Math.ceil(f.length()/(double)CH_SIZE);
		String resp_json = this.createNewDoc(chunk_num);
		if (resp_json.equals("")) return null;
		JSONObject jo = new JSONObject(resp_json);
		// Read document in chunks and upload them as attachments.
        try {
	        byte[] buffer = new byte[CH_SIZE];
	        in = new FileInputStream(f);
            for (int i = 0; in.read(buffer) != -1 ; i++){
            	jo = this.sendChunk(jo, buffer, i);
	        }
        } catch (Exception e) {
        	e.printStackTrace();
        } finally { 
             if ( in != null ) in.close();
             System.out.println("Upload finished!");
        }
        return jo;
	}

	// Creates a doc with random id
	public String createNewDoc(int chunk_num) {
		try {
			String doc_id = "a" + UUID.randomUUID().toString();
			URL url = new URL("http://" + URL + ":" + PORT + "/"
							  + DB_NAME + "/" + doc_id);
			HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
			httpCon.setDoOutput(true);
			httpCon.setRequestMethod("PUT");
			httpCon.setRequestProperty("Content-Type", "application/json");
			OutputStreamWriter out = new OutputStreamWriter(
			    httpCon.getOutputStream());
			out.write("{\"chunks\":" + chunk_num + "}");
			out.close();
			InputStream response = httpCon.getInputStream();
			String resp_string = convertStreamToString(response);
			response.close();
			return resp_string;
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	// Sends a byte array as an individual attachment
	public JSONObject sendChunk(JSONObject jo, byte[] chunk, int chunkN) throws Exception{
		URL url = new URL("http://" + URL + ":" + PORT + "/" + DB_NAME + "/"
						  + jo.getString("id") + "/chunk" + chunkN + ".txt");
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("If-Match", jo.getString("rev"));
		ByteArrayOutputStream out = (ByteArrayOutputStream) httpCon.getOutputStream();
		out.write(chunk);
		out.close();
		InputStream response = httpCon.getInputStream();
		String resp_json = convertStreamToString(response);
		response.close();
		JSONObject jo_new = new JSONObject(resp_json);
		return jo_new;
	}
	
	public FileOutputStream receiveChunk(String doc_id, String rev_id, int chunkN, FileOutputStream fs) throws Exception{
		URL url = new URL("http://" + URL + ":" + PORT + "/" + DB_NAME + "/"
				  + doc_id + "/chunk" + chunkN + ".txt");
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("If-Match", rev_id);
		InputStream response = httpCon.getInputStream();
//		FileOutputStream fs = new FileOutputStream("chunk" + chunkN);
		if (fs == null){
			fs = new FileOutputStream("out_file.png");
		}
		IOUtils.copy(response,fs);
		response.close();
		return fs;
	}
	
	private int getChunkNum(String doc_id) throws Exception{
		URL url = new URL("http://" + URL + ":" + PORT + "/" +
						  DB_NAME + "/" + doc_id);
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		InputStream response = httpCon.getInputStream();
		String resp_json = convertStreamToString(response);
		response.close();
		JSONObject jo_new = new JSONObject(resp_json);
		return jo_new.getInt("chunks");
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