package my.osmstitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.osmstitch.control.Log;

import org.postgis.LinearRing;
import org.postgis.PGgeometryLW;
import org.postgis.Point;
import org.postgis.Polygon;

public class Tile extends DBObject {
	int grid_x;
	int grid_y;

	double left_lon;
	double bottom_lat;

	long created_timestamp;
	long updated_timestamp;

	Polygon geom;

	public static final double BIG_TILE_SIZE = .1;
	public static final double SMALL_TILE_SIZE = .02;
	public static final int BUFFER_SIZE = 1;
	
	boolean still_downloading = false;
	
	boolean force = false;
	

	
	
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
	 * @param lon
	 * @param lat
	 */
	public Tile(double lon, double lat){
		this.grid_x = (int) Math.floor(lon / BIG_TILE_SIZE);
		this.grid_y = (int) Math.floor(lat / BIG_TILE_SIZE);

		this.left_lon = grid_x*BIG_TILE_SIZE;
		this.bottom_lat = grid_y*BIG_TILE_SIZE;
		this.geom = constructGeom();
	}


	public Tile(ResultSet rs){
		try{
			this.grid_x = rs.getInt("grid_x");
			this.grid_y = rs.getInt("grid_y");
			
			this.left_lon = this.grid_x * BIG_TILE_SIZE;
			this.bottom_lat = this.grid_y * BIG_TILE_SIZE;
			
			this.created_timestamp = rs.getLong("created_timestamp");
			this.updated_timestamp = rs.getLong("updated_timestamp");
			this.still_downloading = rs.getBoolean("still_downloading");
			
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
		// TODO Auto-generated method stub
		return "tmp_schema.tiles";
	}

	@Override
	public void setId(long id) {
		//Nothing to do - tile does not have a single ID
		return;
	}

	@Override
	public String getInsertStatement() {
		// TODO Auto-generated method stub
		String sql = "INSERT INTO " + getTableName() + " (grid_x, grid_y, left_lon, bottom_lat, created_timestamp, updated_timestamp, still_downloading, geom)"
				+ " VALUES(" + grid_x + "," + grid_y + "," + left_lon + "," + bottom_lat + "," + created_timestamp + "," + updated_timestamp
				+ ", " + still_downloading + ", 'SRID=4326;" + geom.toString() + "');";
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

		// TODO Auto-generated method stub
		String csv = grid_x + "|" + grid_y + "|" + left_lon + "|"  + bottom_lat + "|" + created_timestamp + "|" + updated_timestamp + "|"
		+ still_downloading + "|" + geomString + "\n";
		return csv;
	}


	public String fileName(){
		return "map/tile_" + grid_x + "_" + grid_y + ".osm";
	}


	@Override
	public int getServerBufferSize() {
		// TODO Auto-generated method stub
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


	public boolean isStill_downloading() {
		return still_downloading;
	}


	public void setStill_downloading(boolean still_downloading) {
		this.still_downloading = still_downloading;
	}





}
