package my.awesomestitch.mapobjects;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;




import my.awesomestitch.control.Log;

import org.postgis.Geometry;
import org.postgis.PGgeometryLW;
import org.postgis.Point;



/**
 * Represents a manually-added node, for counting at non-intersections (like long stretches of highway).
 * @author brian
 *
 */
public class CountingNode extends DBObject {
	

	/**
	 * Unique identifier.  Should be a negative number to distinguish from node ids.
	 */
	private long id;
	
	/**
	 * Human-readable name for this CountingNode.
	 */
	private String name;
	
	/**
	 * A link that this CountingNode allows us to count across.
	 */
	private long link1_id;
	
	/**
	 * Optional second link that this CountingNode allows us to count across. This should be a link heading
	 * in the opposite direction as link1 - either a 2-way street or a divided highway.
	 */
	private long link2_id;
	
	/**
	 * How far along link1 this CountingNode appears (at the beginning, halfway, etc...)
	 */
	private double offset_ratio1;
	
	/**
	 * How far along link2 this CountingNode appears (at the beginning, halfway, etc...)
	 */
	private double offset_ratio2;
	
	/**
	 * The physical geometry (latitude, longitude) of this CountingNodej.
	 */
	private Point geom;
	
	/**
	 * Time when this CountingNode was first created.
	 */
	private long birth_timestamp;
	
	/**
	 * Time when this CountingNode became outdated.  Occurs when one of the underlying links is modified or deleted.
	 */
	private long death_timestamp;
	
	/**
	 * Not stored in the db.  This is used to temporarily store the name of the attached Link when versioning is occuring.
	 * This way, even when underlying links change, we can attempt to re-add a CountingNode at the same location.
	 */
	private String name_filter;
	
	/**
	 * A distance in meters.  Used while generating CountingNodes - We cannot create a CountingNode that is further than
	 * MAX_ERROR_DISTANCE away from the nearest link.
	 */
	public static final double MAX_ERROR_DISTANCE = 1000;

	/**
	 * The two links that we can count on should be facing in opposite directions.
	 * This constant determines what angle is "close enough" to opposite.
	 */
	public static final float OPPOSITE_LINK_THRESHOLD = 150;
	
	
	public CountingNode(){
		this.link1_id = Link.INVALID_LINK_ID;
		this.link2_id = Link.INVALID_LINK_ID;
		this.offset_ratio1 = -1;
		this.offset_ratio2 = -1;
		this.useNegativeId = true;
	}
	
	public CountingNode(ResultSet rs){
		try{
			this.id = rs.getLong("counting_node_id");
			this.name = rs.getString("name");
			this.link1_id = rs.getLong("link1_id");
			this.link2_id = rs.getLong("link2_id");
			this.offset_ratio1 = rs.getDouble("offset_ratio1");
			this.offset_ratio2 = rs.getDouble("offset_ratio2");
			this.birth_timestamp = rs.getLong("birth_timestamp");
			this.death_timestamp = rs.getLong("death_timestamp");
			
			
			//Retrieve geometry object and convert it to the appropriate form
			//TODO : There has to be a better way to do this...
			this.geom = (Point)rs.getObject("geom");
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}
	
	
	@Override
	public String getTableName() {
		return "tmp_schema.counting_nodes";
	}

	@Override
	public void setId(long id) {
		this.id = id;
	}

	@Override
	public String getInsertStatement() {
		
		
		String sql = "INSERT INTO " + getTableName() + " (counting_node_id, name, link1_id, link2_id, offset_ratio1, offset_ratio2, geom, birth_timestamp, death_timestamp)" +
				" VALUES(" + id + ",'" + name + "'," + link1_id + "," + link2_id + "," + offset_ratio1 + "," + offset_ratio2 + "," + "'SRID=4326;POINT(" + geom.x + " " + geom.y + ")'" +
				"," + birth_timestamp + "," + death_timestamp + ");";
		return sql;
	}

	@Override
	public String getHighestIdQuery() {
		// TODO Auto-generated method stub
		return "SELECT MIN(counting_node_id) FROM " + getTableName() + ";";
	}

	@Override
	public String getCSVLine() {
		PGgeometryLW lw = new PGgeometryLW(geom);
		String geom_string = lw.getValue();
		
		String csv = id + "|" + name + "|" + link1_id + "|" + link2_id + "|" + offset_ratio1 + "|" + offset_ratio2 + "|" + geom_string + "|" + birth_timestamp + "|" + death_timestamp;
		return csv;
	}

	@Override
	public int getServerBufferSize() {
		// TODO Auto-generated method stub
		return 50;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getLink1_id() {
		return link1_id;
	}

	public void setLink1_id(long link1_id) {
		this.link1_id = link1_id;
	}

	public long getLink2_id() {
		return link2_id;
	}

	public void setLink2_id(long link2_id) {
		this.link2_id = link2_id;
	}

	public double getOffset_ratio1() {
		return offset_ratio1;
	}

	public void setOffset_ratio1(double offset_1) {
		this.offset_ratio1 = offset_1;
	}

	public double getOffset_ratio2() {
		return offset_ratio2;
	}

	public void setOffset_ratio2(double offset_2) {
		this.offset_ratio2 = offset_2;
	}

	public Point getGeom() {
		return geom;
	}

	public void setGeom(Point geom) {
		this.geom = geom;
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

	public long getId() {
		return id;
	}

	public static double getMaxErrorDistance() {
		return MAX_ERROR_DISTANCE;
	}

	public static float getOppositeLinkThreshold() {
		return OPPOSITE_LINK_THRESHOLD;
	}

	public String getName_filter() {
		return name_filter;
	}

	public void setName_filter(String name_filter) {
		this.name_filter = name_filter;
	}

}
