import java.io.IOException;
import java.lang.Exception;

import org.json.JSONObject;

class ChunkedTransfer{
	public static void main(String[] args) {
		Client c = new Client();
		try {
			JSONObject jo = c.createNewDoc();
			JSONObject new_jo = c.sendChunkedFile("file1.png", jo.getString("id"), jo.getString("rev"));
			c.receiveChunkedFile(new_jo.getString("id"), new_jo.getString("rev"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}