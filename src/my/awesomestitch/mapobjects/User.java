package my.awesomestitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.awesomestitch.control.Log;
import my.awesomestitch.mapobjects.DBObject;



/**
 * @author Brian Donovan <briandonovan100@gmail.com>
 * 
 * Represents a TrafficTurk user.  Has methods to create new Users and validate existing ones.
 *
 */
public class User extends DBObject{
	/**
	 * Unique user id
	 */
	long user_id;
	/**
	 * The username
	 */
	String username;
	/**
	 * This user's password
	 */
	String password;
	/**
	 * The user's email address
	 */
	String email;
	/**
	 * The user's phone number
	 */
	String phone_number;
	/**
	 * The user's first name
	 */
	String first_name;
	/**
	 * The user's last name
	 */
	String last_name;


	private static final int SERVER_BUFFER_SIZE = 50;


	/**
	 * Constructs a user directly from values passed in as arguments
	 * @param username		the user's username
	 * @param password		the user's password
	 * @param email			the user's email address
	 * @param phone_number	the user's phone number
	 * @param first_name	the user's first name
	 * @param last_name		the user's last name
	 */
	public User(String username, String password, String email, String phone_number, String first_name, String last_name){
		//this.user_id = getNextId();
		this.username = username;
		this.password = password;
		this.email = email;
		this.phone_number = phone_number;
		this.first_name = first_name;
		this.last_name = last_name;
	}

	/**
	 * Simpler constructor which only takes a username and password, leaving all other fields blank
	 * @param username	the user's username
	 * @param password	the user's password
	 */
	public User(String username, String password){
		this.username = username;
		this.password = password;
	}

	/**
	 * Constructs a user from one row of an SQL query result. The assumption was that the resultset
	 * was generated from an appropriate query and that rs.next() has been called the appropriate number
	 * of times.
	 * @param rs	a ResultSet generated from a query of the form "SELECT * FROM schema_name.users;"
	 */
	public User(ResultSet rs){
		try{
			this.user_id = rs.getInt("user_id");
			this.username = rs.getString("username");
			this.password = rs.getString("password");
			this.email = rs.getString("email");
			this.phone_number = rs.getString("phone_number");
			this.first_name = rs.getString("first_name");
			this.last_name = rs.getString("last_name");
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}




	/**
	 * returns the name of the database table that contains User objects. Essentially just
	 * concatenates ".users" onto the end of the current schema name.
	 * @return
	 */
	@Override
	public String getTableName(){
		return "tmp_schema.users";
	}


	@Override
	public void setId(long id) {
		this.user_id = id;

	}


	@Override
	public String getInsertStatement() {
		//build query to insert this user
		//query has form
		//INSERT INTO schema_name.nodes (user_id, username, password, email, phone_number, first_name, last_name)
		//VALUES (?,?,?,?,?,?,?);
		
		// String query = "INSERT INTO " + getTableName() + " (user_id, username, password, email, phone_number, first_name, last_name)"+
		//		"VALUES (" + user_id + ", '" + username + "', '" + password + "', '" + email + "', '" + phone_number + "', '" +
		//		first_name + "', '" + last_name + "');";
		
		String query = "INSERT INTO " + getTableName() + " (user_id, username, password, email, phone_number, first_name, last_name)"+
				"VALUES (" + user_id + ", ?, ?, ?, ?, ?, ?)";
		return query;
	}


	@Override
	public String getHighestIdQuery() {
		return "SELECT MAX(user_id) FROM " + getTableName() +";";
	}


	@Override
	public String getCSVLine() {
		String line= user_id + "," + username + "," + password + "," + email + "," + phone_number + ","
				+ first_name + "," + last_name + "\n";
		return line;
	}


	@Override
	public int getServerBufferSize() {
		return SERVER_BUFFER_SIZE;
	}


	//Getters and setters

	public long getUser_id() {
		return user_id;
	}
	public void setUser_id(long id){
		this.user_id = id;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPhone_number() {
		return phone_number;
	}
	public void setPhone_number(String phone_number) {
		this.phone_number = phone_number;
	}
	public String getFirst_name() {
		return first_name;
	}
	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}
	public String getLast_name() {
		return last_name;
	}
	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}


}

