package my.awesomestitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.awesomestitch.control.Log;
import my.awesomestitch.control.MapDownloaderThread;

import org.postgis.LinearRing;
import org.postgis.PGgeometryLW;
import org.postgis.Point;
import org.postgis.Polygon;

public class Tile extends DBObject {
	
	/**
	 * Size of map tiles in degrees lat/lon.  Big tiles are downloaded from OSM and saved in the DB.
	 */
	public static final double BIG_TILE_SIZE = .1;
	
	/**
	 * Size of map tiles in degrees lat/lon.  Small tiles should evenly divide into big tiles,
	 * and can be conveniently queried via DBConnection.tileQuery()
	 */
	public static final double SMALL_TILE_SIZE = .02;
	
	/**
	 * Tiles should be immediately pushed to DB.
	 */
	public static final int BUFFER_SIZE = 1;
	
	/**
	 * Folder in which to save temporary OSM files.
	 */
	public static final String MAP_DIR = "tmp_map";
	
	/**
	 * A constant that specifies the Tile is waiting to be processed.
	 */
	public static final int WAITING = 0;
	
	/**
	 * A constant that specifies the Tile is currently being processed
	 */
	public static final int IN_PROGRESS = 1;
	
	/**
	 * A constant that specifies the Tile is done being processed.
	 */
	public static final int DONE = 2;
	
	/**
	 * The x-cordinate of this tile, corresponds to longitude
	 */
	int grid_x;
	
	/**
	 * The y-coordinate of this tile, corresponds to latitude.
	 */
	int grid_y;

	/**
	 * The longitude of the left part of this tile - this is a function of grid_x (and vice versa)
	 */
	double left_lon;
	
	/**
	 * The latitude of the bottom of this tile - this is a function of grid_y (and vice versa)
	 */
	double bottom_lat;

	/**
	 * The time this tile was created
	 */
	long created_timestamp;
	
	/**
	 * The time this tile was last updated
	 */
	long updated_timestamp;

	/**
	 * Stores the geometrical properties of this Tile
	 */
	Polygon geom;
	

	boolean force = false;
	
	/**
	 * Indicates whether the Tile is downloading.  Can be WAITING, IN_PROGRESS, or DONE
	 */
	int download_status = WAITING;
	
	/**
	 * Indicates whether the detailed map of this Tile is processing.  Can be WAITING, IN_PROGRESS, or DONE
	 */
	int detailed_map_status = WAITING;
	
	/**
	 * Indicates whether the processed map of this Tile is processing.  Can be WAITING, IN_PROGRESS, or DONE
	 */
	int processed_map_status = WAITING;
	
	
	/**
	 * Computes the PostGIS geometry of this tile.
	 * @return A 4-vertix Polygon representing this Tile
	 */
	private Polygon constructGeom(){
		Point topLeftCorner = new Point(left_lon, bottom_lat + BIG_TILE_SIZE);
		Point topRightCorner = new Point(left_lon + BIG_TILE_SIZE, bottom_lat + BIG_TILE_SIZE);
		Point bottomLeftCorner = new Point(left_lon, bottom_lat);
		Point bottomRightCorner = new Point(left_lon + BIG_TILE_SIZE, bottom_lat);

		//combine points into a polygon object
		Point[] pointList = {topLeftCorner, topRightCorner, bottomRightCorner, bottomLeftCorner, topLeftCorner};
		LinearRing[] ring = {new LinearRing(pointList)};
		Polygon box = new Polygon(ring);
		return box;
	}
	
	/**
	 * Simple constructor for a Tile object
	 * @param x The x-coordinate of the tile
	 * @param y The y-coordinate of the tile
	 */
	public Tile(int x, int y){
		this.grid_x = x;
		this.grid_y = y;
		
		this.left_lon = grid_x*BIG_TILE_SIZE;
		this.bottom_lat = grid_y*BIG_TILE_SIZE;
		this.geom = constructGeom();
	}

	/**
	 * Constructs a big tile that covers a given longitude, latitude.  These tiles have discrete x and y coordinates,
	 * where x corresponds to the longitude and y corresponds to the latitude.
	 * @param lon The desired left-longitude of the tile
	 * @param lat The desired bottom-latitude of the tile
	 */
	public Tile(double lon, double lat){
		this.grid_x = (int) Math.floor(lon / BIG_TILE_SIZE);
		this.grid_y = (int) Math.floor(lat / BIG_TILE_SIZE);

		this.left_lon = grid_x*BIG_TILE_SIZE;
		this.bottom_lat = grid_y*BIG_TILE_SIZE;
		this.geom = constructGeom();
	}


	/**
	 * Constructs a Tile object from a ResultSet returned by the DB.
	 * The result should come from a statement like "SELECT * FROM tmp_schema.tiles WHERE ..."
	 * @param rs
	 */
	public Tile(ResultSet rs){
		try{
			this.grid_x = rs.getInt("grid_x");
			this.grid_y = rs.getInt("grid_y");
			
			this.left_lon = this.grid_x * BIG_TILE_SIZE;
			this.bottom_lat = this.grid_y * BIG_TILE_SIZE;
			
			this.created_timestamp = rs.getLong("created_timestamp");
			this.updated_timestamp = rs.getLong("updated_timestamp");
			this.download_status = rs.getInt("download_status");
			this.detailed_map_status = rs.getInt("detailed_map_status");
			this.processed_map_status = rs.getInt("processed_map_status");
			
			
			this.geom = constructGeom();
		}
		catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - constructing Tile");
			Log.e(e);
		}
	}


	public Tile(){}
	
	@Override
	public String toString(){
		return "(" + grid_x + " , " + grid_y + ")";
	}

	@Override
	public String getTableName() {
				return "tmp_schema.tiles";
	}

	@Override
	public void setId(long id) {
		//Nothing to do - tile does not have a single ID
		return;
	}

	@Override
	public String getInsertStatement() {
		String sql = "INSERT INTO " + getTableName() + " (grid_x, grid_y, left_lon, bottom_lat, created_timestamp, updated_timestamp, download_status, detailed_map_status, processed_map_status, geom)"
				+ " VALUES(" + grid_x + "," + grid_y + "," + left_lon + "," + bottom_lat + "," + created_timestamp + "," + updated_timestamp
				+ ", " + download_status + "," + detailed_map_status + "," + processed_map_status + ", 'SRID=4326;" + geom.toString() + "');";
		return sql;
	}

	@Override
	public String getHighestIdQuery() {
		// Nothing to do - tile does not have a single ID
		return "SELECT 0;";
	}

	@Override
	public String getCSVLine() {
		geom.setSrid(4326);
		PGgeometryLW lw = new PGgeometryLW(geom);
		String geomString = lw.getValue();

		String csv = grid_x + "|" + grid_y + "|" + left_lon + "|"  + bottom_lat + "|" + created_timestamp + "|" + updated_timestamp + "|"
		+ download_status + "|" + detailed_map_status + "|" + processed_map_status + "|" + geomString + "\n";
		return csv;
	}


	public String fileName(){
		return MAP_DIR + "/tile_" + grid_x + "_" + grid_y + ".osm";
	}


	@Override
	public int getServerBufferSize() {
		return BUFFER_SIZE;
	}


	public int getGrid_x() {
		return grid_x;
	}


	public void setGrid_x(int grid_x) {
		this.grid_x = grid_x;
	}


	public int getGrid_y() {
		return grid_y;
	}


	public void setGrid_y(int grid_y) {
		this.grid_y = grid_y;
	}


	public double getLeft_lon() {
		return left_lon;
	}


	public void setLeft_lon(double left_lon) {
		this.left_lon = left_lon;
	}


	public double getBottom_lat() {
		return bottom_lat;
	}


	public void setBottom_lat(double bottom_lat) {
		this.bottom_lat = bottom_lat;
	}


	public long getCreated_timestamp() {
		return created_timestamp;
	}


	public void setCreated_timestamp(long created_timestamp) {
		this.created_timestamp = created_timestamp;
	}


	public long getUpdated_timestamp() {
		return updated_timestamp;
	}


	public void setUpdated_timestamp(long updated_timestamp) {
		this.updated_timestamp = updated_timestamp;
	}


	public Polygon getGeom() {
		return geom;
	}


	public void setGeom(Polygon geom) {
		this.geom = geom;
	}


	public boolean wasForced() {
		return force;
	}


	public void setForce(boolean force) {
		this.force = force;
	}



	public int getDownload_status() {
		return download_status;
	}


	public void setDownload_status(int download_status) {
		this.download_status = download_status;
	}


	public int getDetailed_map_status() {
		return detailed_map_status;
	}


	public void setDetailed_map_status(int detailed_map_status) {
		this.detailed_map_status = detailed_map_status;
	}


	public int getProcessed_map_status() {
		return processed_map_status;
	}


	public void setProcessed_map_status(int processed_map_status) {
		this.processed_map_status = processed_map_status;
	}


	
	



}
