package my.awesomestitch.control;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import my.awesomestitch.control.DBConnection;
import my.awesomestitch.mapobjects.Tile;
import my.awesomestitch.mapobjects.User;
import my.awesomestitch.mapobjects.UserTile;


public class MapDownloaderThread extends Thread{
	private static int MAX_DOWNLOAD_THREADS = 2;
	private static int thread_num = 1;
	public String name;

	private static List<MapDownloaderThread> runningThreads = new LinkedList<MapDownloaderThread>();
	private static Queue<Tile> toDownloadQueue = new LinkedList<Tile>();
	private static HashSet<Tile> workingSet = new HashSet<Tile>();

	//private final String OSM_SERVER_NAME = "http://jxapi.openstreetmap.org/xapi/api/0.6/map?";
	private final String OSM_SERVER_NAME = "http://overpass.osm.rambler.ru/cgi/xapi_meta?map?";


	Tile tileToDownload = null;
	boolean isComplete = false;

	public MapDownloaderThread(Tile tile){
		tileToDownload = tile;
		this.name = "DL " + thread_num++;
	}


	public void echo(String str){
		Log.v("TILE", name + " : " + str);
	}



	@Override
	public void run(){

		echo("Processing " + tileToDownload);

		//If we were able to get a tile, it must be downloaded and marked for processing
		if(tileToDownload!=null){
			try{
				//Mark this tile as currently downloading
				synchronized(Controller.lock){
					tileToDownload.setDownload_status(Tile.IN_PROGRESS);
					DBConnection.updateTile(tileToDownload);
				}

				echo("Downloading " + tileToDownload);
				downloadTile(tileToDownload);
				
				//Tile successfully downloaded.  Mark it as complete
				synchronized(Controller.lock){
					tileToDownload.setDownload_status(Tile.DONE);
					DBConnection.updateTile(tileToDownload);
				}


			}
			catch(IOException e){
				//Tile failed to download.  Put it back into the waiting state.
				//This means we will try again later.
				echo("Failed " + tileToDownload);
				synchronized(Controller.lock){
					tileToDownload.setDownload_status(Tile.WAITING);
					DBConnection.updateTile(tileToDownload);
				}
			}
		}

		//Mark this thread as complete and start any new threads if necessary
		isComplete = false;
		Controller.startThreadsIfNecessary();

	}

	/**
	 * Tells whether the thread is finished running.  This can happen due to a successful run or an error.
	 * @return True if the thread completed successfully or failed, False if it is still running
	 */
	public boolean isFinished(){
		if(isComplete)
			return true;
		return !super.isAlive();
	}

	private void downloadTile(Tile tile) throws IOException{
		//Extract coordinates from the tile
		double left = tile.getLeft_lon();
		double bottom = tile.getBottom_lat();
		double right = left + Tile.BIG_TILE_SIZE;
		double top = bottom + Tile.BIG_TILE_SIZE;

		//wrapping cases
		if(left > 180 || right > 180){left -= 360;right -= 360;}
		if(left < -180 || right < -180){left += 360; right +=360;}

		long start_time = System.currentTimeMillis();


		//URL makes call to openstreetmap's jxapi library
		String address = OSM_SERVER_NAME + "bbox=" + left + "," + bottom + "," + right + "," + top;


		//The place where we will save the returned .osm file
		String outPath = tile.fileName();

		//Initiate Apache HTTP request
		HttpGet httpGet = new HttpGet(address);
		httpGet.setHeader("Accept", "text/html,application/xhtml+xml,application/xml");

		InputStream in = null;
		OutputStream out = null;
		File tmp = new File(Tile.MAP_DIR);
		tmp.mkdir();

		try{
			HttpClient httpclient = new DefaultHttpClient();
			HttpResponse response = httpclient.execute(httpGet);
			HttpEntity entity = response.getEntity();

			//Read bytes from the http response and write them to the file
			out = new FileOutputStream(outPath);
			if (entity != null) {
				in = entity.getContent();
				int CHUNK_SIZE = 4096;
				byte data[] = new byte[CHUNK_SIZE];
				int count;
				while((count=in.read(data, 0, CHUNK_SIZE)) != -1){
					out.write(data, 0, count);
				}
			}
		}
		catch(IOException e){
			Log.v("OSM", "Failure downloading tile.");
			Log.e(e);
			if(in!=null)
				in.close();
			if(out!=null)
				out.close();
			throw e;
		}

		long stop_time = System.currentTimeMillis();

		long elapsed = (stop_time - start_time) / 1000;

		String partialFile = outPath.split("/")[1];
		Log.v("TILE", "Successfully downloaded " + partialFile + " after " + elapsed + " secs.");

		in.close();
		out.close();

	}



	public static void joinAll(){

		List<MapDownloaderThread> threads = new LinkedList<MapDownloaderThread>(runningThreads);

		for(MapDownloaderThread thread : threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				Log.e(e);
			}
		}
	}




}
