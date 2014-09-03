package my.awesomestitch.control;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.postgis.LinearRing;
import org.postgis.Point;
import org.postgis.Polygon;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.CountingNode;
import my.awesomestitch.mapobjects.DBObject;
import my.awesomestitch.mapobjects.DetailLink;
import my.awesomestitch.mapobjects.DetailLinkMapping;
import my.awesomestitch.mapobjects.DetailNode;
import my.awesomestitch.mapobjects.Link;
import my.awesomestitch.mapobjects.Node;
import my.awesomestitch.mapobjects.Tile;
import my.awesomestitch.mapobjects.User;
import my.awesomestitch.mapobjects.UserTile;





public class DBConnection {

	/**
	 * The Connection object from the java.sql package
	 */
	private static Connection con = null;
	/**
	 * The name of the schema we are currently editing
	 */
	private static String schema = null;



	public static long DISTANT_FUTURE = 40000000000000L;
	/**
	 * A table which maps SQL table names to the buffer of objects which will eventually be
	 * inserted into that table.  In this way, we can keep a separate buffer for each type of
	 * object.
	 */
	static Hashtable<String, LinkedList<DBObject>> bufferTable;

	/**
	 * A table which maps each type of DBObject to the next available ID number of that type.
	 */
	private static Hashtable<String, Long> nextIdTable;

	/**
	 * Accessor method for the con object.
	 * @return the sql connection
	 */
	public static Connection getConnection(){return con;}


	/**
	 * Establishes a connection to a PostGreSQL database.
	 * Only once this method has been called can any SQL queries be executed.
	 * @param dbHostName The HostName where the PostGres DB is running ("eg trafficturk.hydrolab.illinois.edu")
	 * @param dbName	The name of the database that we are connecting to
	 * @param userName	The username used to connect to the database
	 * @param password	The password required to login to the database
	 */
	private static void connect(String dbHostName, String dbName, String userName, String password){
		String url = "jdbc:postgresql://" + dbHostName + "/" + dbName;


		try{
			//Use the JDBC DriverManager to make the connection.
			con = DriverManager.getConnection(url, userName, password);
			chooseSchema(schema);
		}
		catch(SQLException ex){
			Log.v("DB","SQL EXCEPTION!");
			Log.e(ex);
		}
	}


	/**
	 * Establishes a connection to a local (on this machine) PostGreSQL database.
	 * @param dbName The name of the database that we are connecting to
	 * @param userName The username used to connect to the database
	 * @param password The password required to login to the database
	 */
	private static void connect(String dbName, String userName, String password){
		connect("localhost", dbName, userName, password);
	}


	/**
	 * Connect to the DB, using settings loaded from a config file
	 * @param config_fileName The filename, which contains appropriate info
	 * @throws FileNotFoundException If the config file does not exist
	 */
	public static void initialize(String config_fileName) throws FileNotFoundException{
		FileReader fr = new FileReader(config_fileName);
		BufferedReader br = new BufferedReader(fr);

		String line;
		String dbHostName = null;
		String dbName = null;
		String dbUserName = null;
		String dbPassword = null;

		try {
			while((line=br.readLine())!=null){
				String[] toks = line.split("=");
				if(toks[0].equalsIgnoreCase("db_hostname"))
					dbHostName = toks[1].trim();
				else if(toks[0].equalsIgnoreCase("db_name"))
					dbName = toks[1].trim();
				else if(toks[0].equalsIgnoreCase("db_username"))
					dbUserName = toks[1].trim();
				else if(toks[0].equalsIgnoreCase("db_password"))
					dbPassword = toks[1].trim();
				else if(toks[0].equalsIgnoreCase("db_schema"))
					schema = toks[1].trim();

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if(dbHostName==null)
			connect(dbName, dbUserName, dbPassword);
		else
			connect(dbHostName, dbName, dbUserName, dbPassword);

	}


	/**
	 * Chooses an existing schema to access.  Once a schema is chosen, all queries will be performed on
	 * that schema's tables.
	 * @param schemaName the name of the schema to choose.
	 */
	public static boolean chooseSchema(String schemaName){
		schema = schemaName;
		try{
			Statement st = con.createStatement();

			//check if schema exists
			String query = "select table_schema from information_schema.tables where table_schema = '" + schemaName + "';";
			ResultSet rs = st.executeQuery(query);
			while(rs.next()){
				DBConnection.schema = schemaName;
				Log.v("DB", "Set current schema to '" + schemaName + "'");
				return true;
			}
			Log.v("DB","SCHEMA '" + schemaName + "' NOT FOUND");
			createSchema(schemaName);

		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
		}

		return false;
	}

	/**
	 * Accessor for the name of the current schema
	 * @return the schema that we are currently accessing.
	 */
	public static String getSchemaName(){return schema;}

	/**
	 * Creates a schema with all of the tables necessary for TrafficTurk.
	 * For example, the name supplied is "app", it will create tables
	 * app.nodes, app.links, etc...
	 * If a schema with that name already exists, nothing happens.
	 * @param name	the name of the schema that we are creating.
	 */
	public static void createSchema(String name){

		//Create the Schema
		executeUpdate("CREATE SCHEMA " + name + ";");
		
		//Create the tables within this schema
		executeUpdate("CREATE TABLE tmp_schema.changelog ( timestamp BIGINT, nodes_added BIGINT, nodes_deleted BIGINT, nodes_updated BIGINT, nodes_untouched BIGINT, links_added BIGINT, links_deleted BIGINT, links_updated BIGINT, links_untouched BIGINT, file_name CHARACTER VARYING(200), description CHARACTER VARYING(5000) );");
		executeUpdate("CREATE TABLE tmp_schema.nodes ( node_id BIGINT, is_complete BOOLEAN DEFAULT TRUE, num_in_links SMALLINT, num_out_links SMALLINT, osm_traffic_controller CHARACTER VARYING(100), geom geometry('POINT', 4326), osm_changeset BIGINT, birth_timestamp BIGINT, death_timestamp BIGINT, UNIQUE (node_id, birth_timestamp) );");
		executeUpdate("CREATE TABLE tmp_schema.detail_nodes ( node_id BIGINT, is_complete BOOLEAN DEFAULT TRUE, num_in_links SMALLINT, num_out_links SMALLINT, osm_traffic_controller CHARACTER VARYING(100), geom geometry('POINT', 4326), osm_changeset BIGINT, birth_timestamp BIGINT, death_timestamp BIGINT, UNIQUE (node_id, birth_timestamp) );");
		executeUpdate("CREATE TABLE tmp_schema.counting_nodes ( counting_node_id BIGINT, name CHARACTER VARYING(200), link1_id BIGINT, link2_id BIGINT, offset_ratio1 NUMERIC(8,3), offset_ratio2 NUMERIC(8,3), geom geometry('POINT', 4326), birth_timestamp BIGINT, death_timestamp BIGINT, CONSTRAINT counting_nodes_pkey PRIMARY KEY (counting_node_id)  );");
		executeUpdate("CREATE TABLE tmp_schema.links ( link_id BIGINT, begin_node_id BIGINT NOT NULL, end_node_id BIGINT NOT NULL, begin_angle NUMERIC(5,2), end_angle NUMERIC(5,2), street_length NUMERIC(8,3), osm_name CHARACTER VARYING(100), osm_class CHARACTER VARYING(30), osm_way_id BIGINT, geom geometry('LINESTRING', 4326), osm_changeset BIGINT, birth_timestamp BIGINT, death_timestamp BIGINT, UNIQUE (link_id, birth_timestamp), UNIQUE (begin_node_id, end_node_id, birth_timestamp) );");
		executeUpdate("CREATE TABLE tmp_schema.detail_links ( link_id BIGINT, proc_link_id BIGINT, begin_node_id BIGINT NOT NULL, end_node_id BIGINT NOT NULL, begin_angle NUMERIC(5,2), end_angle NUMERIC(5,2), street_length NUMERIC(8,3), osm_name CHARACTER VARYING(100), osm_class CHARACTER VARYING(30), osm_way_id BIGINT, geom geometry('LINESTRING', 4326), osm_changeset BIGINT, birth_timestamp BIGINT, death_timestamp BIGINT, UNIQUE (link_id, birth_timestamp) );");
		executeUpdate("CREATE TABLE tmp_schema.detail_link_mapping ( link_id BIGINT, detail_link_id BIGINT, osm_way_id BIGINT, birth_timestamp BIGINT, death_timestamp BIGINT );");
		executeUpdate("CREATE TABLE tmp_schema.tiles ( grid_x INTEGER, grid_y INTEGER, left_lon NUMERIC, bottom_lat NUMERIC, created_timestamp BIGINT, updated_timestamp BIGINT, download_status int, detailed_map_status int, processed_map_status int, geom geometry('POLYGON', 4326) );");
		executeUpdate("CREATE TABLE tmp_schema.user_tiles ( user_id BIGINT, grid_x INTEGER, grid_y INTEGER, ordered_timestamp BIGINT, owned_timestamp BIGINT, UNIQUE (user_id, grid_x, grid_y) );");
		executeUpdate("CREATE TABLE tmp_schema.users ( user_id BIGINT, username CHARACTER VARYING(100) NOT NULL, password CHARACTER VARYING(100), email CHARACTER VARYING(100), phone_number CHARACTER VARYING(40), first_name CHARACTER VARYING(100), last_name CHARACTER VARYING(100), UNIQUE (username), CONSTRAINT users_pkey PRIMARY KEY (user_id) );");
		executeUpdate("CREATE TABLE tmp_schema.extravars (name CHARACTER VARYING, val CHARACTER VARYING, UNIQUE(name) );");

		
		
		//Create Indexes - these become very important if the map gets big
		executeUpdate("CREATE INDEX nodes_id ON tmp_schema.nodes USING hash(node_id);");
		executeUpdate("CREATE INDEX nodes_geom_gist ON tmp_schema.nodes USING GIST (geom);");
		executeUpdate("CREATE INDEX nodes_birth_timestamp on tmp_schema.nodes USING btree(birth_timestamp);");
		executeUpdate("CREATE INDEX nodes_death_timestamp on tmp_schema.nodes USING btree(death_timestamp);");

		executeUpdate("CREATE INDEX detail_nodes_id ON tmp_schema.detail_nodes USING hash(node_id);");
		executeUpdate("CREATE INDEX detail_nodes_geom_gist ON tmp_schema.detail_nodes USING GIST (geom);");
		executeUpdate("CREATE INDEX detail_nodes_birth_timestamp on tmp_schema.detail_nodes USING btree(birth_timestamp);");
		executeUpdate("CREATE INDEX detail_nodes_death_timestamp on tmp_schema.detail_nodes USING btree(death_timestamp);");

		executeUpdate("CREATE INDEX links_id ON tmp_schema.links USING hash(link_id);");
		executeUpdate("CREATE INDEX links_way_id ON tmp_schema.links USING hash(osm_way_id);");
		executeUpdate("CREATE INDEX links_geom_gist ON tmp_schema.links USING GIST (geom);");
		executeUpdate("CREATE INDEX links_birth_timestamp on tmp_schema.links USING btree(birth_timestamp);");
		executeUpdate("CREATE INDEX links_death_timestamp on tmp_schema.links USING btree(death_timestamp);");

		executeUpdate("CREATE INDEX detail_links_id ON tmp_schema.detail_links USING hash(link_id);");
		executeUpdate("CREATE INDEX detail_links_way_id ON tmp_schema.detail_links USING hash(osm_way_id);");
		executeUpdate("CREATE INDEX detail_links_geom_gist ON tmp_schema.detail_links USING GIST (geom);");
		executeUpdate("CREATE INDEX detail_links_birth_timestamp on tmp_schema.detail_links USING btree(birth_timestamp);");
		executeUpdate("CREATE INDEX detail_links_death_timestamp on tmp_schema.detail_links USING btree(death_timestamp);");

	}

	/**
	 * Immediately inserts this object into the database - buffering is avoided altogether.
	 * This method has a lower throughput than insertLater, but can be used for more urgent
	 * items.
	 * @param dbo - Any object that overrides the DBObject abstract class
	 */
	public static void insertNow(DBObject dbo){
		//first make sure the id is unique
		updateId(dbo);
		//get the sql statment to insert this object
		String sql = dbo.getInsertStatement();
		sql = sql.replace("tmp_schema", DBConnection.getSchemaName());

		//execute it
		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			String tname = dbo.getTableName().replace("tmp_schema", DBConnection.getSchemaName());

			if(e.getSQLState().equals("23505")){
				//uniqueness violation - that is ok
				Log.v("DB","SQL Exception on insert into: " + tname + "--uniqueness");
				Log.v("DEBUG", sql);
			}
			else{
				Log.v("DB",sql);
				Log.v("DB","SQL Exception on insert into: " + tname);
				Log.v("DB","Code : " + e.getSQLState());
				Log.e(e);
			}
		}

	}




	/**
	 * Enqueues an object to be inserted into the database later.  For each type of object, a list of
	 * uninserted objects is maintained.  When this list gets too full or flush() is called, they will
	 * be bulk inserted into the database and the list will be emptied.
	 * @param dbo - Any object that overrides the DBObject abstract class
	 */
	public static void insertLater(DBObject dbo){
		String tableName = dbo.getTableName();
		//If this is the first time using the buffer table, instantiate it
		if(bufferTable==null){
			bufferTable = new Hashtable<String, LinkedList<DBObject>>();
		}

		LinkedList<DBObject> buffer;
		synchronized(bufferTable){
			if(!bufferTable.containsKey(tableName)){
				//If no buffer exists for this table yet, make it
				buffer = new LinkedList<DBObject>();
				bufferTable.put(tableName, buffer);
			}
			else{
				//Otherwise get the buffer associated with this table
				buffer = bufferTable.get(tableName);
			}
		}

		synchronized(buffer){
			//first make sure the id number is valid
			updateId(dbo);
			//add this object to the buffer - it will eventually be flushed to the database
			buffer.addLast(dbo);

			//if the buffer is full, then flush it
			if(buffer.size() >= dbo.getServerBufferSize())
				flush(tableName);
		}

	}

	/**
	 * Empties a buffer of objects into the appropriate table.  First a bulk-insert will be attempted
	 * but if this fails (probably due to a constraint) we will try to insert the objects one by one
	 * so the valid ones will still make it to the database, and the invalid ones will fail.
	 * <p>This could happen, for example, if we try to insert a bounding box which partially overlaps
	 * a previous bounding box.  The duplicated nodes will cause the ENTIRE update to fail - we want
	 * to ignore these, but still insert the non-duplicate nodes.
	 * @param tableName the name of the table that we are flushing to - this will determine which buffer
	 * that we are going to flush.
	 */
	public static void flush(String tableName) {

		//if this buffer (or the entire buffer table) has not been created yet, just return
		//because there is obviously nothing to flush
		if(bufferTable==null)
			return;

		if(!bufferTable.containsKey(tableName))
			return;

		//get the relevant buffer
		LinkedList<DBObject> buffer = bufferTable.get(tableName);
		synchronized(buffer){
			//try bulk-insert first
			try{
				//concatenate objects into one large CSV table
				String csv = "";
				for(DBObject dbo : buffer){
					csv += dbo.getCSVLine();
				}


				//set up copy statement, using a stream that reads from our big String
				CopyManager cpMan = ((PGConnection)con).getCopyAPI();
				InputStream is = new ByteArrayInputStream(csv.getBytes());
				String trueTableName = tableName.replace("tmp_schema", DBConnection.getSchemaName());

				cpMan.copyIn("COPY " + trueTableName + " FROM STDIN WITH CSV DELIMITER AS '|';", is);
				//Log.v("DB","Bulk upload successful for " + tableName);
			}
			catch(IOException e){
				//something in the bulk insert failed - probably some integrity constraint
				//we will now try to insert the objects one by one in case some of them are valid

				//Log.v("DB","IOException : Bulk insert failed on table " + tableName);
				Log.e(e);
				//insert object one by one. Some may fail, but others may succeed
				for(DBObject dbo : buffer)
					insertNow(dbo);


			}
			catch(SQLException e){
				//Log.v("DB","Bulk upload NOT successful for " + tableName);
				if(e.getSQLState().equals("23505")){
					//uniqueness violation - that is ok
					Log.v("DB","SQLException : Bulk insert failed on table " + tableName + " --uniqueness");

				}
				else{
					Log.v("DB","SQLException : Bulk insert failed on table " + tableName);
					Log.e(e);
				}

				//insert object one by one. Some may fail, but others may succeed
				for(DBObject dbo : buffer)
					insertNow(dbo);
			}
			//the buffer associated with this table will now be empty
			buffer.clear();
		}

	}

	/**
	 * Get next available id for this type of object.  The first time this method is executed,
	 * this is performed by checking the highest-valued id in the database table.  After that, it 
	 * simply increments the id by 1 each time.
	 * @param dbo - Any object that overrides the DBObject class
	 */
	private static void updateId(DBObject dbo) {
		//If this object has been marked with genIdOnInsert=false, we simply return instead of
		//generating a new id.
		if(!dbo.getGenIdOnInsert())
			return;

		//If the nextIdTable does not exist, create it
		if(nextIdTable==null)
			nextIdTable = new Hashtable<String, Long>();

		synchronized(nextIdTable){
			Long next_id = 0l;
			if(!nextIdTable.containsKey(dbo.getTableName())){
				//If the nextIdTable has no entry for this table, we create it by querying the database
				try{
					//query the database for the highest link id
					Statement st = DBConnection.getConnection().createStatement();
					String query = dbo.getHighestIdQuery();
					if(query!= null){
						query = query.replace("tmp_schema", DBConnection.getSchemaName());
						ResultSet rs = st.executeQuery(query);
						Log.v("DEBUG", query);
						rs.next();

						//our next id is that id + 1
						if(!dbo.usesNegativeId())
							next_id = new Long(rs.getLong(1) + 1);
						else
							next_id = new Long(rs.getLong(1) - 1);
						//Log.v("DEBUG", "Got id " + next_id);
					}

				}
				catch(SQLException e){
					Log.v("DB","SQL EXCEPTION - couldn't query " + dbo.getTableName() + " for highest entry");
					if(!dbo.usesNegativeId())
						next_id = new Long(1);
					else
						next_id = new Long(-1);
				}
				catch(NullPointerException e){
					Log.v("DB","NULL POINTER EXCEPTION");
					Log.e(e);
					if(!dbo.usesNegativeId())
						next_id = new Long(1);
					else
						next_id = new Long(-1);
				}
			}
			else{
				//We already have an entry for this id number - get it
				next_id = nextIdTable.get(dbo.getTableName());
			}

			//now that we have figured out the next id number, assign it to this object
			dbo.setId(next_id.longValue());

			//increment the id number and add it back to the table
			if(!dbo.usesNegativeId())
				nextIdTable.put(dbo.getTableName(), new Long(next_id + 1));
			else
				nextIdTable.put(dbo.getTableName(), new Long(next_id - 1));
		}

	}



	/**
	 * A simple wrapper for SQL queries.  Errors are logged, but otherwise ignored.
	 * @param sql - A String that represents the sql query, typically beginning with "SELECT" 
	 * @return - A ResultSet which represents the result of the query
	 */
	public static ResultSet executeQuery(String sql){
		sql = sql.replace("tmp_schema", DBConnection.getSchemaName());

		try {
			Statement st = DBConnection.getConnection().createStatement();
			ResultSet rs = st.executeQuery(sql);
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Log.v("DB", "SQL Exception");
			Log.v("DB", sql);
			Log.e(e);
			return null;
		}
	}
	
	/**
	 * A simple wrapper for SQL updates.
	 * @param sql
	 */
	public static void executeUpdate(String sql){
		sql = sql.replace("tmp_schema", DBConnection.getSchemaName());

		try {
			Statement st = DBConnection.getConnection().createStatement();
			st.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Log.v("DB", "SQL Exception");
			Log.v("DB", sql);
			Log.e(e);
		}
	}

	/**
	 * Adds a new User to the database
	 * @param u - The new User
	 */
	public static void addNewUser(User u){
		insertNow(u);
	}

	/**
	 * Looks up a User by their ID number in the database
	 * @param id The ID of the desired user
	 * @return a User object that matches the ID, or null if no such user exists.
	 */
	public static User lookupUser(long id){
		String sql = "SELECT * FROM " + new User("","").getTableName() + " WHERE "
				+ "user_id = " + id;
		ResultSet rs = DBConnection.executeQuery(sql);

		try {
			if(rs.next())
				return new User(rs);

		} catch (SQLException e) {
			Log.v("DB", "Error looking up user " + id);
			Log.e(e);
		}
		return null;
	}


	/**
	 * Looks up a ContingNode by its ID in the database
	 * @param id The ID nubmer of the CountingNode (should be a negative number)
	 * @return A CountingNode object with the matching ID
	 */
	public static CountingNode getCountingNodeById(long id){
		String tableName = new CountingNode().getTableName().replace("tmp_schema", getSchemaName());
		String query = "SELECT * FROM " + tableName + " WHERE counting_node_id = " + id + ";";
		try{
			Connection con = getConnection();
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(query);
			if(rs.next())
				return new CountingNode(rs);
			else
				return null;
		}
		catch(SQLException e){
			Log.v("DB","SQL Exception - fetching CountingNode");
			Log.v("DB",e.getSQLState());
			Log.e(e);
			return null;
		}
	}


	/**
	 * Creates a manual CountingNode in the database, on top of a link
	 * @param lon - The desired Longitude of the CountingNode
	 * @param lat - The desired latitude of the CountingNode
	 * @param name - A new name for the CountingNode
	 * @param osm_street_name_filter - If this is not null, the search for the nearest link is restricted to those that match the name
	 * @return The generated CountingNode (which has also been added to the DB)
	 */
	public static CountingNode createCountingNode(double lon, double lat, String name, String osm_street_name_filter){
		//First figure out the dimensions of the box we will use for querying
		double latheight = (CountingNode.MAX_ERROR_DISTANCE / (Direction.EARTH_RADIUS * 2 * Math.PI)) * 360;
		double horizontal_radius = Direction.EARTH_RADIUS * Math.cos(Math.toRadians(lat));
		double lonwidth = (CountingNode.MAX_ERROR_DISTANCE / (horizontal_radius * 2 * Math.PI)) * 360;


		//Get a BBox, which contains all nearby links
		BBox box = boundingBoxQuery(lon - lonwidth, lat + latheight, lon + lonwidth, lat - latheight, System.currentTimeMillis(), false, true, true, false);

		//Determine how far this point is from each of the nearby links
		Hashtable<Link, Double> linkDistances = new Hashtable<Link, Double>();
		for(Link link : box.getAllLinks()){
			Point orig = new Point(lon, lat);
			Point proj = link.project(orig);
			if(proj !=null){
				double dist = Direction.haversine(lon, lat, proj.x, proj.y);
				linkDistances.put(link, dist);
			}
		}


		//Determine the closest and second-closest (if any) Links to the given point
		//Note that the second-closest Link is only valid if it is facing the opposite direction
		DetailLink closest = null;
		double closest_dist = 1000000;
		DetailLink second = null;
		double second_dist = 1000000;

		//Find closest link
		for(Link link : linkDistances.keySet()){
			double dist = linkDistances.get(link);
			if(nameMatches(link.getOsm_name(), osm_street_name_filter) && dist < closest_dist){
				closest = (DetailLink) link;
				closest_dist = dist;
			}
		}

		//Find second-closest link which is facing in the opposite direction
		for(Link link : linkDistances.keySet()){
			//boolean matches = osm_street_name_filter==null || link.getOsm_name().equalsIgnoreCase(osm_street_name_filter);
			double dist = linkDistances.get(link);
			if(nameMatches(link.getOsm_name(), osm_street_name_filter) && link != closest && dist < second_dist){
				second = (DetailLink) link;
				second_dist = dist;
			}
		}

		//If no links match, 
		if(closest==null)
			return null;

		CountingNode cn = new CountingNode();
		cn.setName(name);
		long NOW = System.currentTimeMillis();
		cn.setBirth_timestamp(NOW);
		cn.setDeath_timestamp(DISTANT_FUTURE);

		if(second==null){
			//We have only one link for this CountingNode
			//Project the point onto this Link and calculate the offset (distance between CountingNode and link.begin_node
			Point orig = new Point(lon, lat);
			Point proj = closest.project(orig);
			cn.setGeom(proj);
			cn.setLink1_id(closest.getProc_link_id());
			Link tmp = lookupLink(closest.getProc_link_id(), NOW);
			if(tmp==null)
				return null;
			double offset1 =  tmp.offset_ratio(proj);
			cn.setOffset_ratio1(offset1);
		}
		else{
			//We have two links for this CountingNode
			//Project the point onto this Link, then take the midpoint - use this to calculate the offsets
			Point orig = new Point(lon, lat);
			Point proj1 = closest.project(orig);
			Point proj2 = second.project(orig);
			Point midpoint = new Point((proj1.x + proj2.x)/2, (proj1.y + proj2.y)/2);
			cn.setGeom(midpoint);
			Link tmp = lookupLink(closest.getProc_link_id(), NOW);
			double offset1 =  tmp.offset_ratio(midpoint);
			cn.setOffset_ratio1(offset1);
			tmp = lookupLink(second.getProc_link_id(), NOW);
			double offset2 =  tmp.offset_ratio(midpoint);
			cn.setOffset_ratio2(offset2);

			cn.setLink1_id(closest.getProc_link_id());
			cn.setLink2_id(second.getProc_link_id());
		}

		//Insert this CountingNode into the DB and return it
		Log.v("ADMIN", "Inserting countingnode " + cn.getName() + "(" + cn.getGeom().x + "," + cn.getGeom().y + ")");
		insertNow(cn);
		return cn;
	}


	/**
	 * Retrieves from the database all Nodes and Links within a given bounding box.  The query is performed using
	 * PostGis's ST_Intersects() function.  Some of the returned Nodes may not be strictly within the bounding
	 * box - this occurs when they are part of a Link that crosses the edge of the box.  Such Nodes will be marked
	 * as incomplete
	 * @param left		the longitude of the left side of the box
	 * @param top		the latitude of the top of the box
	 * @param right		the longitude of the right side of the box
	 * @param bottom	the latitude of the bottom of the box
	 * @param timestamp the time for which we want to retrieve the map state (the map may be changing over time)
	 * @param processed True if we want the preprocessed map, False if we want the detailed map
	 * @param useSafetyMargin True if we want to also include parts of the map within a "safety margin" of the box.  The safety margin should be used when doing updates to the DB, but not when sending a box to the app.  This prevents us from sending too much unnecessary data at once.
	 * @param includeFullWay - If true, extra links will be included so that the entire Way is used (i.e. no major roads will be "cut in half")
	 * @param includeDetailLinkMapping - If true, extra information is included that stored the mapping between the detailed and processed map
	 * @return			a MapRegion object.  This object contains the lists of relevant Nodes and Links.
	 */
	public static BBox boundingBoxQuery(double left, double top, double right, double bottom, long timestamp, boolean processed, boolean useSafetyMargin, boolean includeFullWay, boolean includeDetailLinkMapping){

		//create corner points based on given dimensions
		//include some safety margin around this box as well
		double effectiveLeft = left;
		double effectiveRight = right;
		double effectiveTop = top;
		double effectiveBottom = bottom;

		if(useSafetyMargin){
			effectiveLeft -= BBox.SAFETY_MARGIN_SIZE;
			effectiveRight += BBox.SAFETY_MARGIN_SIZE;
			effectiveTop += BBox.SAFETY_MARGIN_SIZE;
			effectiveBottom -= BBox.SAFETY_MARGIN_SIZE;
		}

		Point topLeftCorner = new Point(effectiveLeft, effectiveTop);
		Point topRightCorner = new Point(effectiveRight, effectiveTop);
		Point bottomLeftCorner = new Point(effectiveLeft, effectiveBottom);
		Point bottomRightCorner = new Point(effectiveRight, effectiveBottom);

		//combine points into a polygon object
		Point[] pointList = {topLeftCorner, topRightCorner, bottomRightCorner, bottomLeftCorner, topLeftCorner};
		LinearRing[] ring = {new LinearRing(pointList)};
		Polygon box = new Polygon(ring);

		//get relevant table names
		String nodeTableName, linkTableName, countingNodeTableName = null;
		if(processed){
			nodeTableName = (new Node()).getTableName().replace("tmp_schema", getSchemaName());
			linkTableName = (new Link()).getTableName().replace("tmp_schema", getSchemaName());
			countingNodeTableName = (new CountingNode()).getTableName().replace("tmp_schema", getSchemaName());
		}
		else{
			nodeTableName = (new DetailNode()).getTableName().replace("tmp_schema", getSchemaName());
			linkTableName = (new DetailLink()).getTableName().replace("tmp_schema", getSchemaName());
			countingNodeTableName = (new CountingNode()).getTableName().replace("tmp_schema", getSchemaName());
		}


		//setup the BoundingBox object, where we will place all Nodes and Links
		BBox bbox = new BBox(left, top, right, bottom);

		//======================== STEP 1 ====================================
		//Get all of the Links that intersect the Bounding Box (A GiST index is used for fast querying)
		
		
		String query;
		if(!includeFullWay){
			//Simple query - the full way is not desired, so just get links that intersect the box
			query = "SELECT * FROM " + linkTableName + " WHERE ST_Intersects("
					+ linkTableName + ".geom, 'SRID=4326;" + box.toString() + "'::geometry)" +
					"and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";
		}
		else{
			//A more complex query which contains all links that intersect the box, PLUS any additional links that are part of the same Way
			
			//Note the similarity of this subquery to the simple query above
			String subquery = "SELECT osm_way_id FROM " + linkTableName + " WHERE ST_Intersects("
					+ linkTableName + ".geom, 'SRID=4326;" + box.toString() + "'::geometry)" +
					"and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp;

			//Select Links whose way_id matches the way_id of any Link intersecting the box
			query = "SELECT * FROM " + linkTableName + " WHERE osm_way_id in (" + subquery + ")" + 
					" AND birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";
		}

		//Iterate through the results and store all Links in a Set
		Set<Link> linkSet = new HashSet<Link>();
		try{

			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery(query);

			//loop through the resultset, convert each row into a Link object, and add it to the list
			while(rs.next()){
				Link l;
				if(processed)
					l = new Link(rs);
				else
					l = new DetailLink(rs);
				linkSet.add(l);
			}
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}


		// =================================== STEP 2 =======================================
		//also retrieve CountingNodes that intersect the box
		query = "SELECT * FROM " + countingNodeTableName + " WHERE ST_Intersects("
				+ countingNodeTableName + ".geom, 'SRID=4326;" + box.toString() + "'::geometry)" +
				"and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";

		Set<Long> extraLinkIds = new HashSet<Long>();

		//execute query
		try{
			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery(query);

			//loop through the resultset, convert each row into a Node object, and add it to the list
			while(rs.next()){
				CountingNode cn = new CountingNode(rs);
				bbox.add(cn);
				//Keep track of extra links which may need to be loaded
				//(in case this CountingNode is inside the BBox but refers to links slightly outside of the BBox)
				if(cn.getLink1_id()!=Link.INVALID_LINK_ID)
					extraLinkIds.add(cn.getLink1_id());
				if(cn.getLink2_id()!=Link.INVALID_LINK_ID)
					extraLinkIds.add(cn.getLink2_id());

			}
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}


		//=============================  STEP 3 ===================================
		//retrieve extra links that are referred to by the CountingNodes
		//Just in case they fall slightly out of the box
		if(processed){
			if(!extraLinkIds.isEmpty()){
				String linkIdSetString = "(";
				boolean firstElem = true;
				for(long id : extraLinkIds){
					if(!firstElem)
						linkIdSetString += ", ";
					linkIdSetString += id;
					firstElem = false;
				}
				linkIdSetString +=")";

				query = "SELECT * FROM " + linkTableName + " WHERE link_id in " + linkIdSetString +
						" and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";
				//Log.v("DEBUG", query);
				try{
					Statement st = getConnection().createStatement();
					ResultSet rs = st.executeQuery(query);

					//loop through the resultset, convert each row into a Link object, and add it to the list
					while(rs.next()){
						Link l;
						if(processed)
							l = new Link(rs);
						else
							l = new DetailLink(rs);
						linkSet.add(l);
					}
				}
				catch(SQLException e){
					Log.v("DB","SQL EXCEPTION");
					Log.v("DB",e.getSQLState());
					Log.e(e);
				}
			}
		}


		//============================== STEP 4 ==================================
		// Retrieve all nodes that match the desired links (i.e. the set of begin_nodes and end_nodes)
		if(!linkSet.isEmpty()){
			Set<Long> nodeIds = new HashSet<Long>();
			for(Link l : linkSet){
				nodeIds.add(l.getBegin_node_id());
				nodeIds.add(l.getEnd_node_id());
			}
			StringBuilder nodeStringBuilder = new StringBuilder("(");
			//String nodeIdSetString = "(";
			boolean firstElem = true;
			for(long id : nodeIds){
				if(!firstElem)
					nodeStringBuilder.append(", ");
				nodeStringBuilder.append(id);
				firstElem = false;
			}
			nodeStringBuilder.append(")");
			String nodeIdSetString = nodeStringBuilder.toString();

			//System.out.println("Done. executing.");
			//Generate query to select nodes within the box
			//query has form:
			//SELECT * FROM schema_name.nodes where ST_Intersects(schema_name.nodes.geom, 'box_WKT_string');
			query = "SELECT * FROM " + nodeTableName + " WHERE node_id in " + nodeIdSetString +
					" and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";
			//Log.v("DEBUG", query);
			//execute query
			try{
				Statement st = getConnection().createStatement();
				ResultSet rs = st.executeQuery(query);

				//loop through the resultset, convert each row into a Node object, and add it to the list
				while(rs.next()){
					Node n;
					if(processed)
						n = new Node(rs);
					else
						n = new DetailNode(rs);
					bbox.add(n);
				}
			}
			catch(SQLException e){
				Log.v("DB","SQL EXCEPTION");
				Log.v("DB",e.getSQLState());
				Log.e(e);
			}
		}


		//Now that the appropriate nodes are loaded, the Links can be added to the BBox
		for(Link l: linkSet){
			bbox.add(l);
		}


		// ============================== STEP 5 =====================================
		//If desired, also include the mapping between DetailLInks and processed Links
		if(includeDetailLinkMapping){
			String subquery = "SELECT osm_way_id FROM " + linkTableName + " WHERE ST_Intersects("
					+ linkTableName + ".geom, 'SRID=4326;" + box.toString() + "'::geometry)" +
					"and birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp;


			String dlmTable = new DetailLinkMapping().getTableName().replace("tmp_schema", getSchemaName());

			query = "SELECT * FROM " + dlmTable + " WHERE osm_way_id in (" + subquery + ")" + 
					" AND birth_timestamp <= " + timestamp + " and death_timestamp > " + timestamp + ";";

			ResultSet rs = executeQuery(query);

			List<DetailLinkMapping> dlmList = new LinkedList<DetailLinkMapping>();
			try {
				while(rs.next()){
					DetailLinkMapping dlm = new DetailLinkMapping(rs);
					dlmList.add(dlm);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			bbox.setDetailLinkMappings(dlmList);

		}




		Log.v("DB", "Returning " + bbox);
		//STEP 3 - Return the newly created BoundingBox
		return bbox;
	}

	/**
	 * A simpler call than the previous boundingBoxQuery().  Uses false and false as default arguments to the last two arguments (includeFullWay and includeDetailedLinkMapping) 
	 * @param left		the longitude of the left side of the box
	 * @param top		the latitude of the top of the box
	 * @param right		the longitude of the right side of the box
	 * @param bottom	the latitude of the bottom of the box
	 * @param timestamp the time for which we want to retrieve the map state (the map may be changing over time)
	 * @param processed True if we want the preprocessed map, False if we want the detailed map
	 * @param useSafetyMargin True if we want to also include parts of the map within a "safety margin" of the box.  The safety margin should be used when doing updates to the DB, but not when sending a box to the app.  This prevents us from sending too much unnecessary data at once.
	 * @return
	 */
	
	public static BBox boundingBoxQuery(double left, double top, double right, double bottom, long timestamp, boolean processed, boolean useSafetyMargin){
		return boundingBoxQuery(left, top, right, bottom, timestamp, processed, useSafetyMargin, false, false);
	}


	/**
	 * Downloads a portion of the map associated with a given small tile.  This will take the form of a BBox,
	 * which packages all of the Nodes, Links, and CountingNodes.  The BBox will come from the processed map, without including
	 * a safety margin or the full Ways which extend outside of the box.  The reasoning for this is that only the app
	 * uses small tiles, and the app only needs the processed map of the immediately relevant area.
	 * @param small_tile_x The x-coordinate of the relevant small tile
	 * @param small_tile_y The y-coordinate of the relevant small tile
	 * @return A BBox full of Nodes, Links, and CountingNodes if this tile exists, null if it does not.
	 */
	public static BBox tileQuery(int small_tile_x, int small_tile_y){
		//First check that this tile exists.  If it does not, it means we have never downloaded this part of the map,
		//so return null.
		int big_tile_x = (int)Math.round(small_tile_x * Tile.SMALL_TILE_SIZE / Tile.BIG_TILE_SIZE);
		int big_tile_y = (int)Math.round(small_tile_y * Tile.SMALL_TILE_SIZE / Tile.BIG_TILE_SIZE);

		//Fetch the relevant big tile from the DB
		Tile tile = lookupTile(big_tile_x, big_tile_y);
		if(tile!=null && tile.getProcessed_map_status()==Tile.DONE){
			
			//If this tile exists, and it is done being processed, do a BBox query for the rectangle that it defines
			long NOW = System.currentTimeMillis();

			double left = small_tile_x * Tile.SMALL_TILE_SIZE;
			double right = left + Tile.SMALL_TILE_SIZE;

			double bottom = small_tile_y * Tile.SMALL_TILE_SIZE;
			double top = bottom + Tile.SMALL_TILE_SIZE;

			return boundingBoxQuery(left, top, right, bottom, NOW, true, false, false, false);
		}
		else{
			//If this tile does not exist, then return null
			return null;
		}

	}


	/**
	 * Recall that each Node maintains a count of attached in-links and out-links.
	 * This method updates those counts by examining connected links in the DB, for a specified set of Nodes.
	 * @param nodeList The list of Nodes to be updated
	 * @param timeStamp The time at which the update is to be performed.
	 * @param processed True if we are updating the processed map, False if we are updating the detailed map
	 */
	public static void updateNodeLinkCounts(List<Node> nodeList, long timeStamp, boolean processed){
		String nodeTableName, linkTableName;
		if(processed){
			nodeTableName = new Node().getTableName().replace("tmp_schema", getSchemaName());;
			linkTableName = new Link().getTableName().replace("tmp_schema", getSchemaName());;
		}
		else{
			nodeTableName = new DetailNode().getTableName().replace("tmp_schema", getSchemaName());;
			linkTableName = new DetailLink().getTableName().replace("tmp_schema", getSchemaName());;
		}

		StringBuilder sb = new StringBuilder();


		for(Node node : nodeList){
			String sql = "UPDATE " + nodeTableName +  
					" SET num_in_links=(SELECT COUNT(*) FROM " + linkTableName + " WHERE end_node_id=" + node.getId() + "), " +
					"num_out_links=(SELECT COUNT(*) FROM " + linkTableName + " WHERE begin_node_id=" + node.getId() + ") " +
					"WHERE node_id=" + node.getId() + " AND birth_timestamp <=" + timeStamp + " AND death_timestamp >" + timeStamp + ";";
			sb.append(sql);
		}

		String sql = sb.toString();

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Could not update node link_counts.");
			Log.e(e);
		}


	}


	/**
	 * Updates a list of Node death timestamps in the DB
	 * @param nodeList a list of Node objects which have the new correct timestamps
	 * @param processed True if we are editing the processed map, False if we are editing the detailed map
	 */
	public static void updateNodeDeathTimestamps(List<Node> nodeList, boolean processed){
		String tableName;
		if(processed)
			tableName = new Node().getTableName().replace("tmp_schema", getSchemaName());
		else
			tableName = new DetailNode().getTableName().replace("tmp_schema", getSchemaName());

		StringBuilder sb = new StringBuilder();


		for(Node node : nodeList){
			String tmp = "UPDATE " + tableName + " SET death_timestamp = " + node.getDeath_timestamp() + 
					"WHERE node_id = " + node.getId() + " AND birth_timestamp = " + node.getBirth_timestamp() + ";";
			sb.append(tmp);
		}

		String sql = sb.toString();

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Could not update node death_timestamps.");
			Log.e(e);
		}





	}



	/**
	 * Updates a list of Link death timestamps in the DB
	 * @param linkList a list of Link objects, which have the new correct timestamps
	 * @param processed True if we are editing the processed map, False if we are editing the detailed map
	 */
	public static void updateLinkDeathTimestamps(List<Link> linkList, boolean processed){
		String tableName;

		if(processed)
			tableName = new Link().getTableName().replace("tmp_schema", getSchemaName());
		else
			tableName = new DetailLink().getTableName().replace("tmp_schema", getSchemaName());



		StringBuilder sb = new StringBuilder();
		for(Link link : linkList){
			String tmp = "UPDATE " + tableName + " SET death_timestamp = " + link.getDeath_timestamp() + 
					" WHERE link_id = " + link.getId() + " AND birth_timestamp = " + link.getBirth_timestamp() + ";";
			sb.append(tmp);
		}

		String sql = sb.toString();

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Could not update link death_timestamps.");
			Log.e(e);
		}
	}

	/**
	 * Updates the death timestamp of a set of CountingNodes
	 * @param countingNodeList A list of CountingNode objects that contain the correct timestamps
	 */
	public static void updateCountingNodeDeathTimestamps(List<CountingNode> countingNodeList){

		String tableName = new CountingNode().getTableName().replace("tmp_schema", getSchemaName());


		StringBuilder sb = new StringBuilder();
		for(CountingNode cn : countingNodeList){
			String tmp = "UPDATE " + tableName + " SET death_timestamp = " + cn.getDeath_timestamp() + 
					"WHERE counting_node_id = " + cn.getId() + " AND birth_timestamp = " + cn.getBirth_timestamp() + ";";
			sb.append(tmp);
		}

		String sql = sb.toString();

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Could not update countingNode death_timestamps.");
			Log.e(e);
		}


	}

	/**
	 * Updates the death timestamps for a set of DetailLinkMappings
	 * @param dlmList A list of DetialLinkMappings that contain the correct timestamps
	 */
	public static void updateDLMDeathTimestamps(List<DetailLinkMapping> dlmList){
		String tableName = new DetailLinkMapping().getTableName().replace("tmp_schema", getSchemaName());


		StringBuilder sb = new StringBuilder();
		for(DetailLinkMapping dlm : dlmList){
			String tmp = "UPDATE " + tableName + " SET death_timestamp = " + dlm.getDeath_timestamp() + 
					"WHERE link_id = " + dlm.getLink_id() + " AND detail_link_id = " + dlm.getDetail_link_id() + 
					" AND birth_timestamp = " + dlm.getBirth_timestamp() + ";";
			sb.append(tmp);
		}

		String sql = sb.toString();

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Could not update maneuver death_timestamps.");
			Log.e(e);
		}
	}
	
	
	/**
	 * Looks up a Link object by ID
	 * @param id The desired Lonk ID
	 * @param timestamp The time of the desired map (since there are multiple versions of the map)
	 * @return The Link object with the matching ID, or null if no match
	 */
	public static Link lookupLink(long id, long timestamp){
		String sql = "SELECT * FROM " + (new Link().getTableName()) + 
				" WHERE link_id=" + id + " and birth_timestamp <= " + timestamp +
				" and death_timestamp > " + timestamp + ";";
		sql = sql.replace("tmp_schema", getSchemaName());



		try{
			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery(sql);
			//If a result was found, return it. Otherwise return null;
			if(rs.next())
				return new Link(rs);
			else
				return null;
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION - fetching Link by id");
			Log.v("DB",sql);
			Log.e(e);
			return null;
		}
	}

	/**
	 * Find the link_id of the Link that connects two nodes
	 * @param link A Link object with the desired begin_node_id and end_node_id
	 * @param timestamp The time of the map to query (since there can be multiple map versions)
	 * @return The link_id, or Link.INVALID_LINK_ID if no match
	 */
	public static long lookupLinkId(Link link, long timestamp){
		String tablename = link.getTableName();

		String sql = "SELECT link_id FROM " + tablename + 
				" WHERE begin_node_id=" + link.getBegin_node_id() + " AND end_node_id=" + link.getEnd_node_id() +
				" AND birth_timestamp <= " + timestamp + " AND death_timestamp > " + timestamp;

		sql = sql.replace("tmp_schema", getSchemaName());



		ResultSet rs = executeQuery(sql);

		try{
			if(rs.next()){
				return rs.getLong(1);
			}
		}
		catch(SQLException e){
			Log.v("DB", "Error looking up link id");
			Log.v("DB",  sql);
			Log.e(e);
		}

		return Link.INVALID_LINK_ID;
	}

	/**
	 * Lookup a Tile object
	 * @param tile_x - the x-coordinate of the tile (lon / tile_size)
	 * @param tile_y - the y-coordinate of the tile (lat / tile_size)
	 * @return The Tile object, retrieved from the DB
	 */
	public static Tile lookupTile(int tile_x, int tile_y){
		String sql = "SELECT * FROM " + (new Tile(0, 0).getTableName()) + 
				" WHERE grid_x=" + tile_x + " AND grid_y=" + tile_y + ";";
		sql = sql.replace("tmp_schema", getSchemaName());

		//System.out.println(sql);
		ResultSet rs = executeQuery(sql);
		if(rs==null)
			return null;
		try{
			if(rs.next())
				return new Tile(rs);
			else
				return null;
		}catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - fetching Tile by coordinates");
			Log.v("DB", sql);
			Log.e(e);
		}
		return null;
	}
	
	/**
	 * Lookup the list of UserTiles associated with a coordinate.
	 * Recall that the UserTile tells whether a User has a copy of that Tile, is waiting for that Tile, etc...
	 * Thus, multiple UserTiles can exist for each Tile
	 * @param tile_x The x-coordinate of the Tile
	 * @param tile_y The y-coordinate of the Tile
	 * @return a List of UserTile objects corresponding to that tile location
	 */
	public static List<UserTile> lookupUserTiles(int tile_x, int tile_y){
		String sql = "SELECT * FROM " + new UserTile().getTableName() +
				" WHERE grid_x=" + tile_x + " AND grid_y=" + tile_y + ";";
		sql = sql.replace("tmp_schema", getSchemaName());

		List<UserTile> userTiles = new LinkedList<UserTile>();
		ResultSet rs = executeQuery(sql);

		try {
			while(rs.next()){
				UserTile ut = new UserTile(rs);
				userTiles.add(ut);
			}
		} catch (SQLException e) {
			Log.v("DB", "Error fetching UserTiles by (grid_x, grid_y)");
			Log.e(e);
		}


		return userTiles;

	}

	/**
	 * Looks up a list of UserTiles for a given User
	 * @param user_id The ID of the user in question
	 * @return A List of UserTiles for that User
	 */
	public static List<UserTile> lookupUserTiles(long user_id){
		String sql = "SELECT * FROM " + new UserTile().getTableName() +
				" WHERE user_id=" + user_id + ";";
		sql = sql.replace("tmp_schema", getSchemaName());

		List<UserTile> userTiles = new LinkedList<UserTile>();
		ResultSet rs = executeQuery(sql);

		try {
			while(rs.next()){
				UserTile ut = new UserTile(rs);
				userTiles.add(ut);
			}
		} catch (SQLException e) {
			Log.v("DB", "Error fetching UserTiles by user_id.");
			Log.e(e);
		}


		return userTiles;

	}

	/**
	 * Looks up a single UserTile using the minimum amount of unique info
	 * @param user_id The id of the user whom this UserTile belongs to
	 * @param grid_x The x-coordinate of the tile
	 * @param grid_y The y-coordinate of the tile
	 * @return
	 */
	public static UserTile lookupUserTile(long user_id, int grid_x, int grid_y){
		String sql = "SELECT * FROM " + new UserTile().getTableName() +
				" WHERE user_id=" + user_id + " AND  grid_x=" + grid_x + " AND grid_y=" + grid_y + ";";

		sql = sql.replace("tmp_schema", getSchemaName());

		ResultSet rs = executeQuery(sql);

		try {
			if(rs.next()){
				UserTile ut = new UserTile(rs);
				return ut;
			}
		} catch (SQLException e) {
			Log.v("DB", "Error fetching UserTiles by user_id.");
			Log.e(e);
		}


		return null;

	}

	/**
	 * Deletes a UserTile from the DB
	 * @param user_id the id of the User who owns the UserTile
	 * @param grid_x the x-coordinate of the Tile
	 * @param grid_y the y-cooridnate of the Tile
	 */
	public static void deleteUserTile(long user_id, int grid_x, int grid_y){
		String sql = "DELETE FROM " + new UserTile().getTableName() +
				" WHERE " + "user_id=" + user_id + " AND grid_x=" + grid_x + " AND grid_y=" + grid_y + ";";
		sql = sql.replace("tmp_schema", getSchemaName());

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "Couldn't delete UserTile.");
			Log.e(e);
		}
	}


	/**
	 * Deletes a UserTile from the DB
	 * @param ut A UserTile object, containing a valid user_id, grid_x and grid_y
	 */
	public static void deleteUserTile(UserTile ut){
		deleteUserTile(ut.getUser_id(), ut.getGrid_x(), ut.getGrid_y());
	}


	/**
	 * Get the unique set of Tiles corresponding to a list of userTiles
	 * Since multiple UserTiles may correspond to the same Tile
	 * This involves a query to the DB
	 * @param userTiles A list of UserTiles
	 * @return A list of Tiles
	 */
	public static List<Tile> getUniqueTiles(List<UserTile> userTiles){
		Set<List<Integer>> tileCoordSet = new HashSet<List<Integer>>();

		//From the set of UserTiles, determine the unique set of Tiles (determined by grid_x, grid_y)
		//This removes duplicates, in case several users have the same Tile (a very likely scenario)
		for(UserTile ut : userTiles){
			List<Integer> key = new LinkedList<Integer>();
			key.add(ut.getGrid_x());
			key.add(ut.getGrid_y());

			tileCoordSet.add(key);
		}

		//Next, get Tile objects from the DB and add to a list
		List<Tile> tiles = new LinkedList<Tile>();
		for(List<Integer> key : tileCoordSet){
			int x = key.get(0);
			int y = key.get(1);

			Tile t = lookupTile(x,  y);
			tiles.add(t);
		}

		return tiles;
	}


	/**
	 * Look up the set of Tiles which have changed since the last time the User obtained it
	 * @param user_id The ID of the user in question
	 * @return A list of outdated Tiles
	 */
	public static List<Tile> getOutdatedTilesForUser(long user_id){
		List<Tile> outdated = new LinkedList<Tile>();

		String tTab = new Tile().getTableName();
		String utTab = new UserTile().getTableName();

		//Select all Tiles where there exists a corresponding UserTile (same grid_x and grid_y)
		//And this UserTile is older than the Tile.  This means the map was updated AFTER it was sent to the user
		String sql = "SELECT * FROM " + tTab + " WHERE EXISTS("
				+ "SELECT * FROM " + utTab + " WHERE "
				+ utTab + ".grid_x = " + tTab + ".grid_x AND " + utTab + ".grid_y = " + tTab + ".grid_y"
				+ " AND " + utTab + ".owned_timestamp > -1"
				+ " AND " + tTab + ".updated_timestamp > " + utTab + ".owned_timestamp);";

		ResultSet rs = executeQuery(sql);

		try{
			while(rs.next()){
				Tile t = new Tile(rs);
				outdated.add(t);
			}
		}catch(SQLException e){
			Log.v("DB", "Error getting outdated tiles for user " + user_id);
			Log.v("DB", sql);
			Log.e(e);
		}


		return outdated;
	}

	/**
	 * A helper method for doing near-string comparison.  It ignores cases and checks for substrings in either direction
	 * @param name
	 * @param filter
	 * @return
	 */
	private static boolean nameMatches(String name, String filter){
		if(filter==null)
			return true;

		String name_up = name.toUpperCase();
		String filter_up = filter.toUpperCase();

		if(name_up.contains(filter_up) || filter_up.contains(name_up) || name_up.equals(filter_up))
			return true;

		return false;
	}


	/**
	 * Returns the timestamp of the last time the DB was updated
	 * @return A UTC timestamp
	 */
	public static long getLatestUpdateTime(){
		try{
			Statement st = con.createStatement();
			String sql = "SELECT * FROM " + schema + ".extravars WHERE name='latestUpdateTime';";
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				String tmp = rs.getString("val");
				return Long.parseLong(tmp);
			}
		}
		catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - couldn't get latest update time");
			Log.e(e);
		}
		return 0;
	}

	/**
	 * Records the given time into the DB as the last time the DB was updated.
	 * This should be called each tim ethe DB is updated
	 * @param time the current time
	 */
	public static void setLatestUpdateTime(Long time){
		try{
			//first figure out if this row exists or not
			Statement st = con.createStatement();
			String sql = "SELECT * FROM " + schema + ".extravars WHERE name='latestUpdateTime';";
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				//If the row exists, update it to the new time
				sql = "UPDATE " + schema + ".extravars SET val='" + time + "' WHERE name='latestUpdateTime';";
				st.executeUpdate(sql);
			}
			else{
				//If the row does nto exist, create it
				sql = "INSERT INTO " + schema + ".extravars VALUES('latestUpdateTime','" + time + "');";
				st.executeUpdate(sql);
			}
		}
		catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - couldn't set latest update time");
			Log.e(e);
		}

	}
	
	
	/**
	 * Update the updated_timestamp and still_downloading fields of a Tile in the DB
	 * @param t the Tile to update
	 */
	public static void updateTile(Tile t){
		String sql = "UPDATE " + t.getTableName() + " SET " +
				" updated_timestamp=" + t.getUpdated_timestamp() + ", download_status=" + t.getDownload_status() +
				", detailed_map_status=" + t.getDetailed_map_status() + ", processed_map_status=" + t.getProcessed_map_status() +
				" WHERE grid_x=" + t.getGrid_x() + " AND grid_y=" + t.getGrid_y() + ";";
		sql = sql.replace("tmp_schema", getSchemaName());

		try{
			Statement st = con.createStatement();
			st.executeUpdate(sql);
		}
		catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - couldn't update Tile.");
			Log.v("DB", sql);
			Log.e(e);
		}
	}

	/**
	 * How many Tiles are still waiting to download/process in the DB?
	 * @return The number of Tiles still waiting to download/process
	 */
	public static int numTilesWaiting(){
		String sql = "SELECT COUNT(*) FROM " + new Tile(0,0).getTableName() + " WHERE still_downloading=true";
		sql = sql.replace("tmp_schema", getSchemaName());
		try{
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);

			rs.next();
			return rs.getInt(1);

		}
		catch(SQLException e){
			Log.v("DB", "SQL EXCEPTION - couldn't count downloading tiles.");
			Log.v("DB", sql);
			Log.e(e);
		}
		return 0;
	}
	
}
