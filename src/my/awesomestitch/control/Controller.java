package my.awesomestitch.control;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import my.awesomestitch.mapobjects.Tile;
import my.awesomestitch.mapobjects.User;
import my.awesomestitch.mapobjects.UserTile;

public class Controller {

	//Keep track of the necessary threads
	private static int MAX_DOWNLOADER_THREADS = 2;
	private static List<MapDownloaderThread> mapDownloaderThreads = new LinkedList<MapDownloaderThread>();

	private static int MAX_PARSER_THREADS = 2;
	private static List<ParserThread> parserThreads = new LinkedList<ParserThread>();

	private static int MAX_PROCESSOR_THREADS = 2;
	private static List<MapProcessorThread> processorThreads = new LinkedList<MapProcessorThread>();

	private static DBResolverThread dbResolverThread = null;

	/**
	 * The threads that are waiting for the processor threads to finish.
	 */
	private static List<Thread> threadsWaitingToJoin = new LinkedList<Thread>();

	/**
	 * Synchronize on this object when updating tile status.  This prevents DB race conditions.
	 */
	public static final Object lock = new Object();

	public static final Object joinLock = new Object();


	/**
	 * Removes finished threads from the lists of currently running threads.
	 * This way, the lists accurately represent the currently running threads.
	 */
	private static void cleanThreadLists(){
		for(Iterator<MapDownloaderThread> it = mapDownloaderThreads.iterator(); it.hasNext();){
			MapDownloaderThread thread = it.next();
			if(thread.isFinished())
				mapDownloaderThreads.remove(thread);	
		}

		for(Iterator<MapProcessorThread> it = processorThreads.iterator(); it.hasNext();){
			MapProcessorThread thread = it.next();
			if(thread.isFinished())
				processorThreads.remove(thread);	
		}

		for(Iterator<ParserThread> it = parserThreads.iterator(); it.hasNext();){
			ParserThread thread = it.next();
			if(thread.isFinished())
				parserThreads.remove(thread);	
		}

		if(dbResolverThread!= null && dbResolverThread.isFinished())
			dbResolverThread = null;
	}


	/**
	 * This method starts the threads which download, parse, process, and update the enqueued Tiles.
	 * Lists are maintained of the currently running threads, ensuring that a fixed number can run at a time.
	 */
	public static void startThreadsIfNecessary(){
		synchronized(lock){
			//Update the thread lists to accurately represent the currently running threads
			cleanThreadLists();

			//Find Tiles in the DB where download_status==WAITING
			//These are tiles that are queued for download
			//Then create a new MapDownloaderThread to download the .osm file
			try {
				synchronized(mapDownloaderThreads){
					ResultSet rs = DBConnection.executeQuery("SELECT * FROM tmp_schema.tiles WHERE download_status=" + Tile.WAITING + ";");
					while(rs.next() && mapDownloaderThreads.size() < MAX_DOWNLOADER_THREADS){
						Tile tile = new Tile(rs);

						MapDownloaderThread thread = new MapDownloaderThread(tile);
						mapDownloaderThreads.add(thread);

						thread.start();
					}
				}
			} catch (SQLException e) {
				Log.v("DB", "Error searching for tiles queued for download.");
				Log.e(e);
			}

			//If there are any Tiles in the DB where download_status==DONE and detailed_map_status==WAITING
			//These are tiles that are done downloading, and are queued for parsing
			//Then create a new ParserThread to parse the .osm thread
			try{
				synchronized(parserThreads){
					ResultSet rs = DBConnection.executeQuery("SELECT * from tmp_schema.tiles WHERE download_status=" + Tile.DONE+ " AND detailed_map_status=" + Tile.WAITING + ";");

					while(rs.next() && parserThreads.size() < MAX_PARSER_THREADS){
						Tile tile = new Tile(rs);

						ParserThread thread = new ParserThread(tile);
						parserThreads.add(thread);
						thread.start();
					}
				}
			} catch(SQLException e){
				Log.v("DB", "Error searching for tiles queued for parsing.");
			}

			//If all tiles in the DB have detailed_map_status==DONE, then the detailed map is fully created
			//It is now possible to begin preprocessing it and creating the processed map
			//all tiles with processed_map_status==WAITING must be processed
			try{
				synchronized(processorThreads){
					//Query for all tiles that are not done with the detailed map parsing
					ResultSet rs = DBConnection.executeQuery("SELECT * from tmp_schema.tiles WHERE detailed_map_status!=" + Tile.DONE + ";");
					//If none exist, then we can proceed
					if(!rs.next()){
						ResultSet rs2 = DBConnection.executeQuery("SELECT * from tmp_schema.tiles where processed_map_status=" + Tile.WAITING + ";");

						while(rs2.next() && processorThreads.size() < MAX_PROCESSOR_THREADS){
							Tile tile = new Tile(rs2);
							Log.v("DEBUG", "" + tile);
							MapProcessorThread thread = new MapProcessorThread(tile);
							processorThreads.add(thread);
							thread.start();
						}
					}
				}
			} catch(SQLException e){
				Log.v("DB", "Error searching for tiles queued for preprocessing.");
			}

			//If there are any BBoxes in the detailed queue or the processed queue, then they need to be added to the DB
			//So start a new DBResolverThread if there is not already one running
			if(DBResolverThread.hasBBoxesInQueue() && dbResolverThread==null){
				DBResolverThread thread = new DBResolverThread();
				thread.start();
			}


		}
		
		if(finishedRunning()){
			synchronized(joinLock){
				joinLock.notify();
			}
		}


	}



	/**
	 * Adds a new Tile into the processing pipeline by adding it into the DB.  Background threads will then
	 * download the Tile, parse it, preprocess it, and add the map data into the DB.
	 * @param tile The Tile that needs to be downloaded.
	 * @param user The User who is requesting the download.  Useful for sending notifications.
	 * @param force By default, Tiles are ignored if we already have them.  Set this to True if you want to force it to re-download these tiles.
	 * @param runThreads Tells whether or not to run the processing threads.  Recommended setting is True, but one may wish to call enqueue many times, then startNecessaryThreads() ONCE at the end
	 * @return True if the Tile was added to the DB, false if it was ignored (the Tile already exists)
	 */
	public static boolean enqueue(Tile tile, User user, boolean force, boolean runThreads){


		//Now, check if we already have this tile.  We will not re-download it unless force=true
		Tile oldVersion =  DBConnection.lookupTile(tile.getGrid_x(), tile.getGrid_y());
		if(oldVersion!=null && !force){

			if(oldVersion.getProcessed_map_status()!=Tile.DONE)
				Log.v("TILE", "No need to re-download tile " + tile + " - it is already in the queue.");
			else
				Log.v("TILE", "No need to re-download tile " + tile + " - we already have it.  Use force=true to update this tile.");
			//Return false - this tile does not need to be downloaded/processed
			return false;
		}

		if(oldVersion==null){
			tile.setForce(false); // No need to force a tile that we don't have. Forcing is only required to overwrite existing tiles
			//Reset the status of this tile to waiting for download
			tile.setDownload_status(Tile.WAITING);
			tile.setDetailed_map_status(Tile.WAITING);
			tile.setProcessed_map_status(Tile.WAITING);
		}
		else
			tile.setForce(true);



		//Add this tile to the list of tiles which still need to be downloaded
		//Also add it to the workingSet - it will be removed once downloading and processing is complete
		synchronized(lock){
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

		Log.v("TILE", "Enqueued tile " + tile +"");

		//If desired, start the necessary number of threads to continue the processing of the tile
		if(runThreads)
			startThreadsIfNecessary();

		//Return true - we need to wait on this tile
		return true;
	}

	/**
	 * Shortcut to previous method, which sends runThreads=True by default
	 * @param tile The Tile that needs to be downloaded.
	 * @param user The User who is requesting the download.  Useful for sending notifications.
	 * @param force By default, Tiles are ignored if we already have them.  Set this to True if you want to force it to re-download these tiles.
	 * @return True if the Tile was added to the DB, false if it was ignored (the Tile already exists)
	 */
	public static void enqueue(Tile tile, User user, boolean force){
		enqueue(tile, user, force, true);
	}

	/**
	 * Shortcut to previous method which sends force=False by default
	 * @param tile The Tile that needs to be downloaded.
	 * @param user The User who is requesting the download.  Useful for sending notifications.
	 * @param runThreads Tells whether or not to run the processing threads.  Recommended setting is True, but one may wish to call enqueue many times, then startNecessaryThreads() ONCE at the end
	 * @return True if the Tile was added to the DB, false if it was ignored (the Tile already exists)
	 */
	public static void enqueue(Tile tile, User user){
		enqueue(tile, user, false);
	}





	/**
	 * Enqueues an NxN square of Tiles for download.
	 * @param tile_x The x-coordinate of the center Tile
	 * @param tile_y The y-coordinate of the center Tile
	 * @param tile_radius Indicates the size of the square (0=1x1, 1=3x3, 2=5x5, etc...)
	 * @param user The User who is requesting the Tiles.  Useful for sending notifications.
	 * @param force Set to True if you want to overwrite any existing Tiles that intersect the square
	 * @return The number of Tiles that were added (less than or equal to (tile_radius+1)^2 since some tiles may already exist)
	 */
	public static int enqueueSquareOfTiles(int tile_x, int tile_y, int tile_radius, User user, boolean force){
		int numEnqueued = 0;
		//Iterate through the square around this tile
		for(int x = tile_x - tile_radius; x <= tile_x + tile_radius; x++){
			for(int y = tile_y - tile_radius; y <= tile_y + tile_radius; y++){
				Tile tile = new Tile(x,y);
				boolean result = enqueue(tile, user, force, false);
				if(result)
					numEnqueued += 1;
			}
		}

		startThreadsIfNecessary();
		return numEnqueued;

	}

	/**
	 * Enqueues an NxN square of Tiles for download.
	 * @param center_lon A longitude that is within the center Tile of the desired square
	 * @param tile_y The A latitude that is within the center Tile of the desired square
	 * @param tile_radius Indicates the size of the square (0=1x1, 1=3x3, 2=5x5, etc...)
	 * @param user The User who is requesting the Tiles.  Useful for sending notifications.
	 * @param force Set to True if you want to overwrite any existing Tiles that intersect the square
	 * @return The number of Tiles that were added (less than or equal to (tile_radius+1)^2 since some tiles may already exist)
	 */
	public static int enqueueSquare(double center_lon, double center_lat, int tile_radius, User user, boolean force){

		//Create the center tile as a reference point
		Tile centerTile = new Tile(center_lon, center_lat);

		return enqueueSquareOfTiles(centerTile.getGrid_x(), centerTile.getGrid_y(), tile_radius, user, force);
	}

	/**
	 * Indicates whether all threads have finished running.  If they are, then the map is ready to use.
	 * @return True if all threads are finished running, False if not.
	 */
	public static boolean finishedRunning(){
		synchronized(lock){
			cleanThreadLists();

			return (mapDownloaderThreads.size()==0 && parserThreads.size()==0 && processorThreads.size()==0 &&  dbResolverThread==null);
		}
	}

	/**
	 * Blocks until all threads are done running and the map is ready.
	 */
	public static void joinAll(){

		//The method will not execute until all threads are finished running.
		//Technially the loop should only execute once, but it is implemented as
		//a while loop for robustness
		while(!finishedRunning()){
			synchronized(joinLock){
				try {
					joinLock.wait();
				} catch (InterruptedException e) {
					Log.e(e);
				}
			}

		}
	}

}
