package my.awesomestitch.control;

import java.io.FileNotFoundException;

import my.awesomestitch.mapobjects.BBox;

public class OSMStitch {
	public static void main(String[] args){
		try {
			DBConnection.initialize("test_server_config");
		} catch (FileNotFoundException e) {
			Log.v("ERROR", "Config file not found.");
		}
		
		//40.1130° N, 88.2612° W
		Controller.enqueueSquare(-88.26, 40.113, 1, null, false);
		
		Controller.joinAll();
		
		BBox box = DBConnection.getEntireMap(true);
		Plotter.plotMap(box, "tmp_map.pdf");
		
		
		
	}

}
