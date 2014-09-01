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
	public MapDownloaderThread(Tile tile){
		tileToDownload = tile;
		this.name = "DL " + thread_num++;
	}


	//TODO: Move all of this stuff into Controller.java
	/*
	public static boolean enqueue(Tile tile, User user, boolean force){


		//Now, check if we already have this tile.  We will not re-download it unless force=true
		Tile oldVersion =  DBConnection.lookupTile(tile.getGrid_x(), tile.getGrid_y());
		if(oldVersion!=null && !force){

			if(oldVersion.isStill_downloading())
				Log.v("TILE", "No need to re-download tile " + tile + " - it is already in the queue.");
			else
				Log.v("TILE", "No need to re-download tile " + tile + " - we already have it.  Use force=true to update this tile.");
			//Return false - this tile does not need to be downloaded/processed
			return false;
		}

		if(oldVersion==null)
			tile.setForce(false); // No need to force a tile that we don't have. Forcing is only required to overwrite existing tiles
		else
			tile.setForce(true);


		//Add this tile to the list of tiles which still need to be downloaded
		//Also add it to the workingSet - it will be removed once downloading and processing is complete
		synchronized(toDownloadQueue){
			toDownloadQueue.add(tile);
			workingSet.add(tile);

			tile.setStill_downloading(true);
			tile.setCreated_timestamp(System.currentTimeMillis());

			//Create a new UserTile - records that the user has ordered this tile in the DB
			//Later, this information can be used to determine whether a given user has other tiles in the queue
			if(user!=null){
				UserTile ut = new UserTile(user.getUser_id(), tile.getGrid_x(), tile.getGrid_y());
				ut.setOwned_timestamp(UserTile.DNE);
				ut.setOrdered_timestamp(System.currentTimeMillis());

				//If this UserTile already exists in the DB, remove it - it needs to be replaced
				DBConnection.deleteUserTile(ut);
				//Add this order to the DB
				DBConnection.insertNow(ut);
			}

			if(oldVersion==null)
				DBConnection.insertNow(tile);
			else
				DBConnection.updateTile(tile);
		}

		//If needed, start another thread to handle downloading
		synchronized(runningThreads){
			if(runningThreads.size() < MAX_DOWNLOAD_THREADS){
				MapDownloaderThread thread = new MapDownloaderThread();
				//Track this thread in the runningThreads set
				runningThreads.add(thread);
				thread.start();
			}
		}

		Log.v("TILE", "Enqueued tile " + tile +"");

		//Return true - we need to wait on this tile
		return true;
	}

	public static void enqueue(Tile tile, User user){
		enqueue(tile, user, false);
	}





	public static int enqueueSquareOfTiles(int tile_x, int tile_y, int tile_radius, User user, boolean force){
		int numEnqueued = 0;
		//Iterate through the square around this tile
		for(int x = tile_x - tile_radius; x <= tile_x + tile_radius; x++){
			for(int y = tile_y - tile_radius; y <= tile_y + tile_radius; y++){
				Tile tile = new Tile(x,y);
				boolean result = enqueue(tile, user, force);
				if(result)
					numEnqueued += 1;
			}
		}

		return numEnqueued;

	}


public static int enqueueSquare(double center_lon, double center_lat, int tile_radius, User user, boolean force){

		//Create the center tile as a reference point
		Tile centerTile = new Tile(center_lon, center_lat);

		return enqueueSquareOfTiles(centerTile.getGrid_x(), centerTile.getGrid_y(), tile_radius, user, force);
	}






	public static void doneProcessing(Tile tile){
		synchronized(toDownloadQueue){
			workingSet.remove(tile);
		}
	}
	 */

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
				synchronized(Controller.dbTileLock){
					tileToDownload.setDownload_status(Tile.IN_PROGRESS);
					DBConnection.updateTile(tileToDownload);
				}

				echo("Downloading " + tileToDownload);
				downloadTile(tileToDownload);
				//Tile successfully downloaded.

				//Mark this tile as finished downloading
				synchronized(Controller.dbTileLock){
					tileToDownload.setDownload_status(Tile.DONE);
					DBConnection.updateTile(tileToDownload);
				}

			}
			catch(IOException e){
				//Tile failed to download.  Put it back into the waiting state.
				//This means we will try again later.
				echo("Failed " + tileToDownload);
				synchronized(Controller.dbTileLock){
					tileToDownload.setDownload_status(Tile.WAITING);
					DBConnection.updateTile(tileToDownload);
				}
			}
		}

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
