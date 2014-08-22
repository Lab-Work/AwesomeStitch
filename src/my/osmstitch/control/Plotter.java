package my.osmstitch.control;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import my.osmstitch.mapobjects.BBox;
import my.osmstitch.mapobjects.CountingNode;
import my.osmstitch.mapobjects.Link;
import my.osmstitch.mapobjects.Node;


//import my.traffic.turk.osmparser.OSMParser;

public class Plotter {
	public static final int L_NONE = 0;
	public static final int L_NAMES = 1;
	public static final int L_LENGTHS = 2;
	public static final int L_TOSTRING = 3;
	public static final int L_ID = 4;

	public static final int N_NONE = 0;
	public static final int N_ID = 1;
	public static final int N_LINK_COUNTS = 2;


	static Hashtable<Long,String> colormap = new Hashtable<Long,String>();
	static int current_id = 0;
	//static String[] cols = {"yellow","red","green","black","orange","purple","gray","teal","pink"};
	static String[] cols = {"1", "2", "3", "4", "5", "6", "7", "8"};


	public static void main(String[] args){

		//drawDecaturTest();
		//linkCountsTest();
		//dbTest();

	}
/*


	public static void curvyTest(){
		DBConnection.connect("osm", "postgres", "3117newmark");
		DBConnection.dropSchema("plottest");
		DBConnection.createSchema("plottest");
		DBConnection.chooseSchema("plottest");

		OSMParser.insertBoundingBox("resources/brush2.osm", null, "tmp");


		long NOW = System.currentTimeMillis();


	}


	public static void dbTest(){
		DBConnection.connect("trafficturk.hydrolab.illinois.edu", "trafficturk", "postgres", "3117newmark");
		DBConnection.chooseSchema("fps_test4");

		long NOW = System.currentTimeMillis();
		//NOW = 1376267161000l;
		BBox parsedBox = DBConnection.boundingBoxQuery(-88.9022, 39.9144, -88.8822, 39.9015, NOW, true, false);


		BBox detailBox = DBConnection.boundingBoxQuery(-88.9022, 39.9144, -88.8822, 39.9015, NOW, false, false);

		drawMap(parsedBox, "resources/area_parsed.csv", L_LENGTHS, N_NONE);
		drawMap(detailBox, "resources/area_detail.csv", L_LENGTHS, N_NONE);
	}


	public static void countingNodeTest(){
		DBConnection.connect("osm", "postgres", "3117newmark");

		DBConnection.dropSchema("plottest");
		DBConnection.createSchema("plottest");

		DBConnection.chooseSchema("plottest");

		//OSMParser.insertBoundingBox("resources/cn_test.osm", null, "tmp");
		OSMParser.insertBoundingBox("resources/decatur_fixed.osm", null, "tmp");
		OSMParser.insertBoundingBox("resources/cn_test.osm", null, "tmp");


		CountingNode cn = DBConnection.createCountingNode(-88.85985, 39.919364, "72 at Kirby", "I 72");
		Maneuver[] tmp = DBConnection.getPossibleManeuversForCountingNode(cn);

		cn = DBConnection.createCountingNode(-88.836953, 39.937884, "72 at Illiniwick", "I 72");
		tmp = DBConnection.getPossibleManeuversForCountingNode(cn);

		cn = DBConnection.createCountingNode(-88.822726, 39.955264, "72 at Argenta", "I 72");
		tmp = DBConnection.getPossibleManeuversForCountingNode(cn);



		long NOW = System.currentTimeMillis();

		BBox parsedBox = DBConnection.boundingBoxQuery(-88.8846, 39.9631, -88.8046, 39.9118, NOW, true, false);
		BBox detailBox = DBConnection.boundingBoxQuery(-88.8846, 39.9631, -88.8046, 39.9118, NOW, false, false);
		drawMap(parsedBox, "resources/map_parsed.csv", L_ID, N_NONE);
		drawMap(detailBox, "resources/map_detail.csv", L_ID, N_NONE);

	}

	public static void drawDecaturTest(){
		DBConnection.connect("osm", "postgres", "3117newmark");

		DBConnection.chooseSchema("cntest2");



		long NOW = System.currentTimeMillis();

		BBox parsedBox = DBConnection.boundingBoxQuery(-89.0325, 40.0660, -88.7156, 39.8607, NOW, true, false);
		//BBox detailBox = DBConnection.boundingBoxQuery(-89.0356, 39.9690, -88.7187, 39.7634, NOW, false, false);
		drawMap(parsedBox, "resources/decatur_parsed.csv", L_NONE, N_NONE);
		//drawMap(detailBox, "resources/map_detail.csv", L_NONE);
	}
	
	public static void linkCountsTest(){
		DBConnection.connect("osm", "postgres", "3117newmark");

		DBConnection.dropSchema("plottest");
		DBConnection.createSchema("plottest");

		DBConnection.chooseSchema("plottest");
		
		
		OSMParser.insertBoundingBox("resources/count_2.osm", null, "tmp");
		long NOW = System.currentTimeMillis();

		BBox parsedBox = DBConnection.boundingBoxQuery(-88.9638, 39.8962, -88.9440, 39.8833, NOW, true, false);
		BBox detailBox = DBConnection.boundingBoxQuery(-88.9638, 39.8962, -88.9440, 39.8833, NOW, false, false);
		drawMap(parsedBox, "resources/map_parsed.csv", L_ID, N_LINK_COUNTS);
		drawMap(detailBox, "resources/map_detail.csv", L_ID, N_LINK_COUNTS);
		
	}
*/
	public static void drawMap(BBox box, String fileName, int linkLabels, int nodeLabels){

		List<CountingNode> countingNodes = new LinkedList<CountingNode>();
		for(long id : box.countingNodes.keySet()){
			countingNodes.add(box.countingNodes.get(id));
		}


		try{
			//create the CSV file
			FileWriter fw = new FileWriter(fileName);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter pw = new PrintWriter(bw);
			pw.println("type,name,color,startx,starty,endx,endy");

			for(Node n : box.getAllNodes()){
				String nodename = "";
				if(nodeLabels == N_ID)
					nodename = n.getId() + "";
				else if(nodeLabels==N_LINK_COUNTS)
					nodename = "<" + n.getNum_in_links() +"-" + n.getNum_out_links() + ">";
				pw.println("node,"+ nodename + ","+ color(n.getBirth_timestamp()) + "," + n.getGeom().x + "," + n.getGeom().y);
			}

			for(Link l : box.getAllLinks()){
				String linkname = "";
				if(linkLabels == L_NAMES)
					linkname = l.getOsm_name();
				else if(linkLabels== L_LENGTHS)
					linkname = "" + l.getStreet_length();
				else if(linkLabels==L_TOSTRING)
					linkname = l.toString();
				else if(linkLabels==L_ID)
					linkname = l.getId() + "";

				pw.println("link," + linkname + "," + color(l.getBirth_timestamp()) + "," + l.getGeom().getPoint(0).x + "," + l.getGeom().getPoint(0).y + "," + l.getGeom().getPoint(1).x + "," + l.getGeom().getPoint(1).y);
			}

			for(CountingNode cn : countingNodes){
				String nodename = cn.getId() + "";
				//nodename="";
				pw.println("counting_node,"+ nodename + ","+ color(cn.getBirth_timestamp()) + "," + cn.getGeom().x + "," + cn.getGeom().y);
			}

			pw.close();

			//run the R script, which reads the CSV file and generates a picture
			File file = new File(fileName);
			String path = file.getAbsolutePath();
			Runtime.getRuntime().exec("Rscript resources/plotMap.R " + path); 
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	public static String color(long timestamp){
		if(colormap.containsKey(timestamp)){
			return colormap.get(timestamp);
		}
		else{
			String color = cols[current_id];
			colormap.put(timestamp, color);
			current_id = (current_id + 1) % cols.length;
			return color;
		}
	}







}
