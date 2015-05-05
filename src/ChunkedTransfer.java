import java.lang.Exception;

import org.json.JSONException;
import org.json.JSONObject;

class ChunkedTransfer{
	public static void main(String[] args) {
		try {
			JSONObject jo = Client.createNewDoc();
			Client c = new Client(args[0], jo.getString("id"), jo.getString("rev"), Integer.parseInt(args[1]));
			JSONObject new_jo = c.sendChunkedFile(args[0], jo.getString("id"), jo.getString("rev"));
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