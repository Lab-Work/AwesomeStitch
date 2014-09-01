package my.awesomestitch.mapobjects;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import my.awesomestitch.control.Log;

import org.postgis.Geometry;
import org.postgis.PGgeometry;
import org.postgis.PGgeometryLW;
import org.postgis.Point;


/**
 * 
 * @author Brian Donovan <briandonovan100@gmail.com>
 *
 * Represents a Node on the map.  This could be an intersection or some other point of interest.
 */
public class Node extends DBObject{
	
	/**
	 * The unique id of this node from the database - this is also the corresponding node id in OSM
	 */
	protected long node_id;
	
	/**
	 * True if all related Links are also loaded
	 */
	protected boolean is_complete;
	
	/**
	 * True if this Node is the end of a Way in OSM
	 */
	protected boolean isWayEnd = false;
	
	protected short osm_num_ways;
	
	/**
	 * Number of links that flow into the node.
	 */
	protected short num_in_links;
	
	/**
	 * Number of links that flow out of the node.
	 */
	protected short num_out_links;
	
	
	/**
	 * Traffic controller information from OSM
	 */
	protected String osm_traffic_controller;
	
	/**
	 * The geographical geometry of this point, encoded as a PostGIS POINT
	 */
	protected Point geom;
	
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
	

	protected static int SERVER_BUFFER_SIZE = 2000;

	/**
	 * Default constructor
	 */
	public Node(){}
	
	/**
	 * Generates a node directly from values passed in as arguments.
	 * @param isComplete				True if all related links are also loaded
	 * @param node_id					Node id loaded from OSM
	 * @param osm_traffic_controller	Traffic controller info from OSM
	 * @param geom						Physical geometry of this node, encoded as a PostGIS POINT
	 */
	public Node(boolean isComplete, long node_id, String osm_traffic_controller, Point geom){
		this.is_complete = isComplete;
		this.node_id = node_id;
		this.osm_traffic_controller = osm_traffic_controller;
		this.geom = geom;
	}
	
	/**
	 * Constructs a node from an XML tag.  The assumption is that this tag was created from a line
	 * in an OSM planet file.  The tag will contain all of the information needed to generate the Node.
	 * @param tag An XML tag read from an OSM planet file
	 */
	public Node(XMLTag tag){
		this.is_complete = true;
		isWayEnd = false;
		this.node_id = Long.parseLong(tag.getValue("id"));
		this.osm_traffic_controller = "";
		this.osm_changeset = Long.parseLong(tag.getValue("changeset"));
		double lat = Double.parseDouble(tag.getValue("lat"));
		double lon = Double.parseDouble(tag.getValue("lon"));
		geom = new Point(lon, lat);
		
	}
	
	/**
	 * Constructs a Node from one row of an SQL query result.  The assumption was that the resultset
	 * was generated from an appropriate query and that rs.next() has been called the appropriate number
	 * of times.
	 * @param rs	A resultset generated from an SQL query of the form "SELECT * from some_schema.nodes;"
	 */
	public Node(ResultSet rs){
		//extract data from the current row of the resultset
		//using column names
		try{
			this.node_id = rs.getLong("node_id");
			this.is_complete = rs.getBoolean("is_complete");
			this.num_in_links = rs.getShort("num_in_links");
			this.num_out_links = rs.getShort("num_out_links");
			this.osm_traffic_controller = rs.getString("osm_traffic_controller");
			this.osm_changeset = rs.getLong("osm_changeset");
			this.birth_timestamp = rs.getLong("birth_timestamp");
			this.death_timestamp = rs.getLong("death_timestamp");
			
			
			PGgeometry g = (PGgeometry)rs.getObject("geom");
			Geometry geo = g.getGeometry();
			this.geom = geo.getPoint(0);
			
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}
	
	
	public Node(Node n){
		this.node_id = n.getId();
		this.is_complete = n.isComplete();
		this.num_in_links = n.getNum_in_links();
		this.num_out_links = n.getNum_out_links();
		this.osm_traffic_controller = n.getOsm_traffic_controller();
		this.osm_changeset = n.getOsm_changeset();
		this.birth_timestamp = n.getBirth_timestamp();
		this.death_timestamp = n.getDeath_timestamp();
		this.geom = new Point(n.getGeom().x, n.getGeom().y);
		this.isWayEnd = n.isWayEnd();
	}

	
	//DBObject Methods
	/**
	 * Returns the name of the database table where Node objects are stored.  Essentially, it 
	 * just concatenates ".nodes" to the end of the current schema name
	 * @return the name of the nodes table in the database
	 */
	@Override
	public String getTableName(){
		return "tmp_schema.nodes";
	}

	@Override
	public String getInsertStatement() {
		
		if(osm_traffic_controller.equals(""))
			osm_traffic_controller=null;
		
		String query = "INSERT INTO " + getTableName() + " (node_id, is_complete, num_in_links, num_out_links, osm_traffic_controller, geom, " +
				"osm_changeset, birth_timestamp, death_timestamp)"+
				" VALUES(" + node_id + ", " + is_complete + ", " + num_in_links + ", " + num_out_links + ", " + osm_traffic_controller + ", " +
				"'SRID=4326;POINT(" + geom.x + " " + geom.y + ")'" + ", " + osm_changeset + "," + birth_timestamp + ", " + death_timestamp + ");";
		return query;
	}

	@Override
	public String getHighestIdQuery() {
		return "SELECT MAX(node_id) FROM " + getTableName() + ";";
	}

	@Override
	public String getCSVLine() {
		geom.srid=4326;
		PGgeometryLW tmp = new PGgeometryLW(geom);
		String geom_string = tmp.getValue();
		
		String line = node_id + "|" + is_complete + "|" + num_in_links + "|" + num_out_links + "|" + 
				osm_traffic_controller + "|" + geom_string + "|" + osm_changeset + "|" + birth_timestamp + "|" + death_timestamp + "\n";
		//System.out.println(line);
		return line;
	}

	@Override
	public int getServerBufferSize() {
		return SERVER_BUFFER_SIZE;
	}
	
	public String toString(){
		return "" + node_id;
	}
	
	
	
	//Getters and setters
	
	public long getId() {
		return node_id;
	}

	public void setId(long id) {
		this.node_id = id;
	}

	public boolean isComplete() {
		return is_complete;
	}

	public void setComplete(boolean isComplete) {
		this.is_complete = isComplete;
	}

	public short getOsm_num_ways() {
		return osm_num_ways;
	}
	public void setOsm_num_ways(short osm_num_ways) {
		this.osm_num_ways = osm_num_ways;
	}
	public void incrementOsm_num_ways(){
		this.osm_num_ways++;
	}

	public String getOsm_traffic_controller() {
		return osm_traffic_controller;
	}

	public void setOsm_traffic_controller(String osm_traffic_controller) {
		this.osm_traffic_controller = osm_traffic_controller;
	}

	public Point getGeom() {
		return geom;
	}

	public void setGeom(Point geom) {
		this.geom = geom;
	}

	public boolean isWayEnd() {
		return isWayEnd;
	}

	public void setWayEnd(boolean isWayEnd) {
		this.isWayEnd = isWayEnd;
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

	public short getNum_in_links() {
		return num_in_links;
	}

	public void setNum_in_links(short num_in_links) {
		this.num_in_links = num_in_links;
	}

	public short getNum_out_links() {
		return num_out_links;
	}

	public void setNum_out_links(short num_out_links) {
		this.num_out_links = num_out_links;
	}


	public static void main(String[] args){
		
	}

	
}
