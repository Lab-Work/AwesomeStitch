package my.awesomestitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


/**
 * Represents an object which can be inserted or extracted from the PostGres database.
 * 
 * @author Brian Donovan <briandonovan100@gmail.com>
 *
 */
public abstract class DBObject {

	/**
	 * Controls the behavior of the updateId() method
	 * If true, it will generate a unique id sequentially
	 * If false, it will do nothing - this assumes that the id has been set correctly
	 */
	private boolean genIdOnInsert = true;
	
	protected boolean useNegativeId = false;
	
	/**
	 * Setter for the genIdOnInsert field
	 * @param val The boolean value, which indicates whether the DB should auto-generate an
	 * ID for this object when it is inserted.
	 */
	public void setGenIdOnInsert(boolean val){
		genIdOnInsert = val;
	}
	
	public boolean getGenIdOnInsert(){
		return genIdOnInsert;
	}
	
	
	public boolean usesNegativeId(){
		return useNegativeId;
	}

	/**
	 * Gets the name of the relevant table in the database
	 * @return the name of the relevant table in the database
	 */
	public abstract String getTableName();


	/**
	 * Gets the next available unique id number for this object - this may involve querying the database,
	 * but usually just involves incrementing a counter.
	 */
	public abstract void setId(long id);

	/**
	 * Gets an SQL statement that will insert this object into the database
	 * @return the SQL statement as a strong
	 */
	public abstract String getInsertStatement();

	/**
	 * Gets an SQL query which asks for the highest id-number of any object of this type
	 * This will help us assign unique id numbers to each type of object.
	 * @return
	 */
	public abstract String getHighestIdQuery();

	/**
	 * Gets this object in CSV form - useful for COPY statements
	 * @return the fields of this object in a String, separated by commas, with a \n on the end
	 */
	public abstract String getCSVLine();

	/**
	 * The size of buffer that the DBConnection should use to store objects of this type.
	 * @return
	 */
	public abstract int getServerBufferSize();


}
