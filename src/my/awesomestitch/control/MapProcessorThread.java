package my.awesomestitch.control;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
//import java.util.Set;

//import javax.mail.Message;
//import javax.mail.MessagingException;
//import javax.mail.Session;
//import javax.mail.Transport;
//import javax.mail.internet.AddressException;
//import javax.mail.internet.InternetAddress;
//import javax.mail.internet.MimeMessage;

import my.awesomestitch.control.DBConnection;
import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.Tile;
//import my.awesomestitch.mapobjects.User;
//import my.awesomestitch.mapobjects.UserTile;


public class MapProcessorThread extends Thread {
	private static Queue<Tile> toProcessQueue = new LinkedList<Tile>();

	public static List<Tile> processingOrDownloading = new LinkedList<Tile>();

	private static List<MapProcessorThread> runningThreads = new LinkedList<MapProcessorThread>();

	String name;
	static int thread_num = 1;
	Tile tileToProcess = null;
	boolean isComplete = false;





	public MapProcessorThread(Tile tile){
		this.name = "PC " + thread_num++;
		this.tileToProcess = tile;
	}

	public void echo(String str){
		Log.v("TILE", name + " : " + str);
	}



	@Override
	public void run(){


		echo("got " + tileToProcess);

		synchronized(Controller.lock){
			tileToProcess.setProcessed_map_status(Tile.IN_PROGRESS);
			DBConnection.updateTile(tileToProcess);
		}
		
		
		//If we were able to get a tile, it must be downloaded and marked for processing
			//Get the coordinates of this tile and the current time
			double left = tileToProcess.getLeft_lon();
			double top = tileToProcess.getBottom_lat() + Tile.BIG_TILE_SIZE;
			double right = tileToProcess.getLeft_lon() + Tile.BIG_TILE_SIZE;
			double bottom = tileToProcess.getBottom_lat();
			long NOW =System.currentTimeMillis();

			//Load the newest version of the detailed map from the DB
			BBox newDetailedMap = DBConnection.boundingBoxQuery(left, top, right, bottom, NOW, false, true, true, true);

			//Perform the preprocessing - this is a fairly heavy computation
			BBox newProcessedMap = newDetailedMap.preprocess();
			newProcessedMap.setTile(tileToProcess);

			//Add the processed tile to the list of DB updates
			synchronized(Controller.lock){
				DBResolverThread.enqueueProcessedBBox(newProcessedMap);
			}




		//Mark this thread as complete and start any new threads if necessary
		isComplete = true;
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

	
}
