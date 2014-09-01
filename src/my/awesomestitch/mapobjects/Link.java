package my.awesomestitch.mapobjects;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import my.awesomestitch.control.Direction;
import my.awesomestitch.control.Log;

import org.postgis.Geometry;
import org.postgis.LineString;
import org.postgis.PGgeometry;
import org.postgis.PGgeometryLW;
import org.postgis.Point;

@SuppressWarnings("serial")
/**
 * 
 * @author Brian Donovan <briandonovan100@gmail.com>
 * 
 * Represents a Link between two nodes on the map.  Has methods for inserting and retrieving Links from the
 * database.
 *
 */
public class Link extends DBObject implements Serializable{
	/**
	 * The unique id of this link from the database
	 */
	protected long id;

	/**
	 * The node that this link is coming from
	 */
	protected long begin_node_id;

	/**
	 * The node that this link is going to
	 */
	protected long end_node_id;

	/**
	 * The geographical angle of the street at the beginning node
	 */
	protected float begin_angle;

	/**
	 * The geographical angle of the street at the end node
	 */
	protected float end_angle;


	/**
	 * Geographical length of this link
	 */
	protected double street_length;

	/**
	 * Street name imported from OSM
	 */
	protected String osm_name;

	/**
	 * Type of street imported from OSM (highway, residential, etc...)
	 */
	protected String osm_class;

	/**
	 * The way that this link was a part of in OSM
	 */
	protected long osm_way_id;

	/**
	 * The physical geometry of this link, as a PostGIS LINESTRING object
	 */
	protected LineString geom;

	/**
	 * The changeset ID imported from osm - a larger number is a more recent change.
	 */
	protected long osm_changeset;

	/**
	 * The time at which an update first created this node in our DB
	 */
	protected long birth_timestamp;

	/**
	 * The time at which an update deprecated this node in our DB
	 */
	protected long death_timestamp;

	protected static final int SERVER_BUFFER_SIZE = 2000;
	protected static double LINK_ANGLE_RATIO = .1;

	public static final long INVALID_LINK_ID = -1;


	/**
	 * Used by the method getShortenedName() - These entries represent replacements for the purpose of shortening a street name.
	 */
	private static final String[] name_shorteners = {"Crossing:Cr", "Blacktop:", "Illinois:IL", "Highway:HW", "Crescent:Cs", "Close:Cl",
		"Boulevard:Blv", "Lane:Ln", "Road:Rd", "Avenue:Ave", "Street:St", "South:S", "North:N", "East:E", "West:W"};
	/**
	 * Default constructor
	 */
	public Link(){}


	public void updateAngles(){
		Point p1 = geom.getPoint(0);
		Point p2 = geom.getPoint(1);

		

		this.begin_angle = (float) Direction.azimuth(p1.x, p1.y, p2.x, p2.y);
		this.end_angle = (float) Direction.azimuth(p2.x, p2.y, p1.x, p1.y);



	}

	/**
	 * Generates a link directly from values passed in as arguments.
	 * 
	 * @param begin_node	The node object that the Link begins at
	 * @param end_node		The node object that the Link ends at
	 * @param osm_name		The street name imported from OSM
	 * @param osm_class		The street type imported from OSM
	 * @param osm_way_id	The id of the Way this Link was a part of in OSM
	 */
	public Link(Node begin_node, Node end_node, String osm_name, String osm_class, long osm_way_id) {
		this.begin_node_id = begin_node.getId();
		this.end_node_id = end_node.getId();
		this.osm_name = osm_name;
		this.osm_class = osm_class;
		this.osm_way_id = osm_way_id;
		Point[] points = {begin_node.getGeom(), end_node.getGeom()};
		geom = new LineString(points);
		updateAngles();
	}

	public Link(long linkId, long begin_node_id, long end_node_id, float begin_angle, float end_angle, String name){
		this.id = linkId;
		this.begin_node_id = begin_node_id;
		this.end_node_id = end_node_id;
		this.begin_angle = begin_angle;
		this.end_angle = end_angle;
		this.osm_name = name;
	}

	/**
	 * Constructor for generating a link from one row of an SQL query result.  It assumes that rs was generated 
	 * from an appropriate query and that rs.next() has been called the appropriate number of times.
	 * @param rs	a resultset generated from a SQL query of form "SELECT * FROM some_schema.Links;"
	 */
	public Link(ResultSet rs){
		try{
			this.id = rs.getLong("link_id");
			this.begin_node_id = rs.getLong("begin_node_id");
			this.end_node_id = rs.getLong("end_node_id");
			this.begin_angle = rs.getFloat("begin_angle");
			this.end_angle = rs.getFloat("end_angle");
			this.street_length = rs.getDouble("street_length");
			this.osm_name = rs.getString("osm_name");
			this.osm_class = rs.getString("osm_class");
			this.osm_way_id = rs.getLong("osm_way_id");
			this.osm_changeset = rs.getLong("osm_changeset");
			this.birth_timestamp = rs.getLong("birth_timestamp");
			this.death_timestamp = rs.getLong("death_timestamp");

			//retrieve geometry
			//TODO :  there has to be a better way to do this
			PGgeometry g = (PGgeometry)rs.getObject("geom");
			Geometry geo = g.getGeometry();
			this.geom = (LineString) geo;
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}

	public Link(Link l){
		this.id = l.getId();
		this.begin_node_id = l.getBegin_node_id();
		this.end_node_id = l.getEnd_node_id();
		this.begin_angle = l.getBegin_angle();
		this.end_angle = l.getEnd_angle();
		this.street_length = l.getStreet_length();
		this.osm_name = l.getOsm_name();
		this.osm_class = l.getOsm_class();
		this.osm_way_id = l.getOsm_way_id();
		this.osm_changeset = l.getOsm_changeset();
		this.birth_timestamp = l.getBirth_timestamp();
		this.death_timestamp = l.getDeath_timestamp();
		LineString g = l.getGeom();
		Point[] points = {new Point(g.getPoint(0).x, g.getPoint(0).y),new Point(g.getPoint(1).x, g.getPoint(1).y)}; 
		this.geom = new LineString(points);
	}

	/**
	 * Uses the Haversine formula to calculate the length of the Link, given the latitude and longitude of its endpoints
	 * http://en.wikipedia.org/wiki/Haversine_formula
	 * http://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
	 * @return A distance in meters
	 */
	public double haversine(){
		return Direction.haversine(geom.getPoint(0).x, geom.getPoint(0).y, geom.getPoint(1).x, geom.getPoint(1).y);
	}

	@Override
	public String getTableName() {
		return "tmp_schema.links";
	}



	@Override
	public String getInsertStatement() {
		// TODO Auto-generated method stub
		//Build SQL query from the data within this link
		//Query has form:
		//INSERT INTO schema_name.link (link_id , begin_node_id , end_node_id , begin_angle , end_angle , osm_name ,
		//	osm_class , osm_way_id , osm_begin_node_id , osm_end_node_id , geom)
		//	VALUES(?,?,?,?,?,?,?,?,?,?,'LINESTRING(? ?,? ?)');

		//Single quotes ' cause problems for SQL, but they are escaped as ''
		//so we have to replace all instances of ' with '' before the insert statement will work.

		String tmp_osm_name;
		if(osm_name==null || osm_name.equals(""))
			tmp_osm_name = null;
		else
			tmp_osm_name = osm_name.replace("'", "''");

		String query = "INSERT INTO " + getTableName() + " (link_id , begin_node_id , end_node_id , begin_angle , end_angle , street_length, osm_name ," +
				" osm_class , osm_way_id , geom, osm_changeset, birth_timestamp, death_timestamp) VALUES(" +
				id + "," + begin_node_id + "," + end_node_id +", " + begin_angle + ", " + end_angle + ", " +
				street_length + ", '" + tmp_osm_name +"','" + osm_class + "'," + osm_way_id + "," +
				"'SRID=4326;LINESTRING(" + geom.getPoint(0).x + " " + geom.getPoint(0).y + "," + geom.getPoint(1).x + " " + geom.getPoint(1).y + ")', " +
				osm_changeset + ", " + birth_timestamp + ", " + death_timestamp + ");";
		return query;
	}


	/**
	 * Returns an SQL query - this query will give us the highest link_id which exists in the table
	 */
	@Override
	public String getHighestIdQuery() {
		return "select max(link_id) from " + getTableName() + ";";
	}

	@Override
	public String getCSVLine() {
		//first get geometry data string for this object
		geom.srid=4326;
		PGgeometryLW lw = new PGgeometryLW(geom);
		String geom_string = lw.getValue();
		//Now concatenate all fields together into a CSV line
		String line = id + "|" + begin_node_id + "|" + end_node_id + "|" + begin_angle + "|" + end_angle + "|" +
				street_length + "|" + osm_name + "|" + osm_class + "|" + osm_way_id  + "|" + geom_string + "|" + osm_changeset + "|" +
				birth_timestamp + "|" + death_timestamp + "\n";
		return line;
	}

	@Override
	public int getServerBufferSize() {
		// TODO Auto-generated method stub
		return SERVER_BUFFER_SIZE;
	}


	public boolean equals(Object o){
		Link other = (Link)o;
		if(this.begin_node_id==other.getBegin_node_id() && this.end_node_id==other.getEnd_node_id()){
			return true;
		}
		return false;
	}

	public int hashCode(){
		long v3 = (this.begin_node_id + 7*this.end_node_id)%117281;

		return (int)v3;

	}

	//Method for serialization
	public String toString(){
		return this.osm_name + " (" + this.begin_node_id + "-->" + this.id + "-->" + this.end_node_id + ")";
	}


	/**
	 * Projects a point onto this link.  Returns null if the projection is not within the span of the link.
	 * Note that this projection is "approximate" since we are dealing with lat-lon coordinates, not Euclidean ones
	 * @param coords A PGpoint that we want to project onto this link.
	 * @return A new PGpoint representing the projected point.
	 */
	public Point project(Point coords){
		//Calculate link length
		double link_dx = (geom.getPoint(1).x - geom.getPoint(0).x);
		double link_dy = (geom.getPoint(1).y - geom.getPoint(0).y);
		double length = Math.sqrt(link_dx*link_dx + link_dy*link_dy);

		//Calculate unit vector for link
		double ux = link_dx / length;
		double uy = link_dy / length;


		//Calculate point vector with respect to link start point
		double px = coords.x - geom.getPoint(0).x;
		double py = coords.y - geom.getPoint(0).y;

		//Cross product = length of projected point
		double dot_prod = ux*px + uy*py;

		//Create new point
		//Scale cross_prod by unit vector and offset by original link points
		Point result = new Point(geom.getPoint(0).x + ux*dot_prod, geom.getPoint(0).y + uy*dot_prod);

		//If the point lies on the line, return it, otherwise return null
		if(result.x > geom.getPoint(0).x && result.x > geom.getPoint(1).x)
			return null;
		if(result.x < geom.getPoint(0).x && result.x < geom.getPoint(1).x)
			return null;

		return result;
	}

	/**
	 * Calculates how "far along" a link some point is. This is expressed as a portion of the total link length.
	 * So, for example, a point right at the link's begin_node will have an offset_ratio of 0, a point in the middle
	 * will have an offset_ratio of .5, and a point at the end_node will have an offset_ratio of .5;
	 * @param coords a PGpoint that we want to check against this link.
	 * @return A ratio describing how far along the link that point is.
	 */
	public double offset_ratio(Point coords){
		//Project onto this link
		Point proj = project(coords);
		//Determine total length of this link
		double totalDist = this.haversine();
		//Determine distance from begin_node to projected point
		double pointDist = Direction.haversine(geom.getPoint(0).x, geom.getPoint(0).y, proj.x, proj.y);
		//Express as ratio
		return pointDist / totalDist;
	}
	
	public static String shorten(String str){
		String tmp = new String(str);
		for(String mapping : name_shorteners){
			String[] toks = mapping.split(":");
			String from = toks[0];
			String to = "";
			if(toks.length > 1)
				to = toks[1];
			tmp = tmp.replaceAll("(?i)" + from, to);
		}
		return tmp;
	}

	public String getShortenedName(){
		return Link.shorten(osm_name);
	}

	//Getters and setters
	public long getId() {
		return id;
	}

	@Override
	public void setId(long id) {
		this.id = id;
	}


	public long getBegin_node_id() {
		return begin_node_id;
	}


	public void setBegin_node_id(long begin_node_id) {
		this.begin_node_id = begin_node_id;
	}


	public long getEnd_node_id() {
		return end_node_id;
	}


	public void setEnd_node_id(long end_node_id) {
		this.end_node_id = end_node_id;
	}


	public float getBegin_angle() {
		return begin_angle;
	}


	public void setBegin_angle(float begin_angle) {
		this.begin_angle = begin_angle;
	}


	public float getEnd_angle() {
		return end_angle;
	}


	public void setEnd_angle(float end_angle) {
		this.end_angle = end_angle;
	}


	public String getOsm_name() {
		return osm_name;
	}


	public void setOsm_name(String osm_name) {
		this.osm_name = osm_name;
	}


	public String getOsm_class() {
		return osm_class;
	}


	public void setOsm_class(String osm_class) {
		this.osm_class = osm_class;
	}


	public long getOsm_way_id() {
		return osm_way_id;
	}


	public void setOsm_way_id(long osm_way_id) {
		this.osm_way_id = osm_way_id;
	}


	public LineString getGeom() {
		return geom;
	}


	public void setGeom(LineString geom) {
		this.geom = geom;
	}


	public long getOsm_changeset() {
		return osm_changeset;
	}


	public void setOsm_changeset(long osm_changeset) {
		this.osm_changeset = osm_changeset;
	}


	public long getBirth_timestamp() {
		return birth_timestamp;
	}


	public void setBirth_timestamp(long birth_timestamp) {
		this.birth_timestamp = birth_timestamp;
	}


	public long getDeath_timestamp() {
		return death_timestamp;
	}


	public void setDeath_timestamp(long death_timestamp) {
		this.death_timestamp = death_timestamp;
	}


	public double getStreet_length() {
		return street_length;
	}


	public void setStreet_length(double street_length) {
		this.street_length = street_length;
	}


}