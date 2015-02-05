import java.io.FileInputStream;
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

class Client {
	// Chunk size
	private static int CH_SIZE = 2048;
	private static String DB_NAME = "potato1";
	private static String URL = "127.0.0.1";
	private static String PORT = "5984";

	public void splitFile(String filename) throws Exception{
		// Create new doc
		FileInputStream in = null;
		String resp_json = this.createNewDoc();
		if (resp_json.equals("")) return;
		JSONObject jo = new JSONObject(resp_json);
		// Read document in chunks and upload them as attachments.
        try {
	        byte[] buffer = new byte[CH_SIZE];
            in = new FileInputStream(filename);
            for (int i = 0; in.read(buffer) != -1 ; i++){
            	jo = this.sendChunk(jo, buffer, i);
	        }
        } catch (Exception e) {
        	e.printStackTrace();
        } finally { 
             if ( in != null ) in.close();
             System.out.println("finished");
        }
	}

	// Creates a doc with random id
	public String createNewDoc() {
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
			out.write("{}");
			out.close();
			InputStream response = httpCon.getInputStream();
			String resp_string = convertStreamToString(response);
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
		JSONObject jo_new = new JSONObject(resp_json);
		return jo_new;
	}
	
	// Read server response into string
	static String convertStreamToString(InputStream in) throws IOException{
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