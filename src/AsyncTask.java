import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;


public class AsyncTask implements Callable<Object>{

	protected URL url;
	protected String doc_id, doc_rev;
	protected long start;
	protected long end;
	
	public AsyncTask(String filename, URL url, String doc_id, String doc_rev, long start, long end) throws IOException {
		this.url = url;
		this.doc_id = doc_id;
		this.start = start;
		this.end = end;
	}
	
	@Override
	public Object call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
