package my.awesomestitch.control;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.CountingNode;
import my.awesomestitch.mapobjects.Link;
import my.awesomestitch.mapobjects.Node;



public class Plotter {
	

	/**
	 * Plots an exported map as a PDF file, using a simple R script.
	 * @param nodeFileName The CSV file where Nodes are saved
	 * @param linkFileName The CSV file where LInks are saved
	 * @param outFileName The PDF file to be output
	 */
	public static void plotMap(String nodeFileName, String linkFileName, String outFileName){
		//Command that plots the map
		String cmd = "Rscript resources/plotMap.R " + nodeFileName + " " + linkFileName + " " + outFileName;
		Log.v("Plotter", cmd);
		try {
			//Run command and wait for it to finish
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
		} catch (IOException e) {
			Log.e(e);
		} catch (InterruptedException e){
			Log.e(e);
		}
		
	}

	/**
	 * Plots a BBox as a PDF file, using a simple R script.
	 * @param box The BBox to be plotted (can be either detailed or processed)
	 * @param outFileName The PDF file to be output.
	 */
	public static void plotMap(BBox box, String outFileName){
		//Generate two temporary CSV files to hold the map
		String nodeFileName = outFileName + "_nodes.csv";
		String linkFileName = outFileName + "_links.csv";
		box.saveToFile(nodeFileName, linkFileName);
		
		//Call the Rscript on these two CSV files
		plotMap(nodeFileName, linkFileName, outFileName);
	}





}
