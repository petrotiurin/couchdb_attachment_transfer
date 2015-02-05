import java.io.IOException;
import java.lang.Exception;
import org.json.JSONObject;

class ChunkedTransfer{
	public static void main(String[] args) {
		Client c = new Client();
		try {
			JSONObject jo = c.sendChunkedFile("file1.png");
			System.out.println("Doc created: " + jo.getString("id"));
			c.receiveChunkedFile(jo.getString("id"), jo.getString("rev"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}