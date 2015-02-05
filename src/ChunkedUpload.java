import java.io.IOException;
import java.lang.Exception;

class ChunkedUpload{
	public static void main(String[] args) throws Exception {
		Client c = new Client();
//		c.splitFile(args[0]);
		c.splitFile("file1.png");
//		System.out.println(c.createNewDoc());
	}
}