import java.io.IOException;
import java.lang.Exception;

import org.json.JSONException;
import org.json.JSONObject;

class ChunkedTransfer{
	public static void main(String[] args) {
		Client c = new Client();
		try {
			JSONObject jo = c.createNewDoc();
			JSONObject new_jo = c.sendChunkedFile("file1.png", jo.getString("id"), jo.getString("rev"));
//			String new_id, new_rev;
//			try {
//				new_id = new_jo.getString("id");
//				new_rev = new_jo.getString("rev");
//			} catch (JSONException e) {
//				new_id = new_jo.getString("_id");
//				new_rev = new_jo.getString("_rev");
//			}
//			c.receiveChunkedFile(new_id, new_rev);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}