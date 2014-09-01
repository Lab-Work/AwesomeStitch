package my.awesomestitch.control;


public class Direction {

	//prevents initialization
	private Direction() {}
	
	//Geographic directions
	public static final int NORTH = 0;
	public static final int EAST = 1;
	public static final int SOUTH = 2;
	public static final int WEST = 3;
		
	
	
	//An array of possible directions
	public static final int[] possibleDirections = {Direction.NORTH, Direction.EAST, Direction.SOUTH, Direction.WEST};
	
	
	//screen directions
	public static final int UP = 0;
	public static final int RIGHT = 1;
	public static final int DOWN = 2;
	public static final int LEFT = 3;
	
	//Mean radius of the earth in meters
	public final static double EARTH_RADIUS = 6371000;
	
	/**
	 * Given a direction that the user is facing, combined with a direction on the screen, we obtain
	 * a true direction.  This is useful for mapping swipes to real-life directions
	 * @param geog_direction The direction that the user and device are facing in the physical world
	 * @param screen_direction The direction of the a swipe on the screen (for example upwards)
	 * @return A real-life direction that is calculated by adding the two directions
	 */
	public static int getTrueDirection(int geog_direction, int screen_direction){
		return (geog_direction + screen_direction) % 4;
	}
	
	/**
	 * Calculates the positive angle between two given angels
	 * This number wraps around 360, and is always less than 180
	 * @param a1 The first given angle
	 * @param a2 The second given angle
	 * @return The angle between these two angles
	 */
	public static float positiveAngularProximity(float a1, float a2) {

		float diff = a1 - a2;
		
		if(diff < 0)
			diff+= 360;
		
		if(diff > 180)
			diff = 360 - diff;
		
		return diff;
	}
	

	
	
	/**
	 * Implementation of the Haversine function - calculates the distance between two points, given their latitudes and longitudes
	 * Assumes a spherical earth.
	 * http://en.wikipedia.org/wiki/Haversine_formula
	 * http://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
	 * @param longitude1
	 * @param latitude1
	 * @param longitude2
	 * @param latitude2
	 * @return A distance in meters
	 */
	public static double haversine(double longitude1, double latitude1, double longitude2, double latitude2){
		double lon1 = Math.toRadians(longitude1);
		double lat1 = Math.toRadians(latitude1);
		
		double lon2 = Math.toRadians(longitude2);
		double lat2 = Math.toRadians(latitude2);
		
		double lat_haversine = Math.sin((lat2-lat1)/2) * Math.sin((lat2-lat1)/2);
		double lon_haversine = Math.sin((lon2 - lon1)/2) * Math.sin((lon2 - lon1)/2);
		double cosine_term = Math.cos(lat1) * Math.cos(lat2);
		
		
		double distance = 2 * EARTH_RADIUS * Math.asin(Math.sqrt(lat_haversine + cosine_term*lon_haversine));
		
		//System.out.println(distance);
		return distance;
	}
	
	
	/**
	 * Returns the angle of travel between two points (measured clockwise from North)
	 * @param longitude1 The longitude of the start position
	 * @param latitude1 The latitude of the start position
	 * @param longitude2 The longitude of the end position
	 * @param latitude2 The latitude of the end position
	 * @return
	 */
	public static double azimuth(double longitude1, double latitude1, double longitude2, double latitude2){
		double lat1 = Math.toRadians(latitude1);
		double lat2 = Math.toRadians(latitude2);
		double dlon = Math.toRadians(positiveAngularProximity((float)longitude1, (float)longitude2));
		if(longitude1 > longitude2)
			dlon *= -1;
		
		double num = Math.sin(dlon);
		double denom = Math.cos(lat1)*Math.tan(lat2) - Math.sin(lat1)*Math.cos(dlon);
		
		double radians = Math.atan2(num, denom);
		
		return  Math.toDegrees(radians);
	}
	
	
	/**
	 * Given two angles calculates the angle in the middle of them.
	 * @param a1
	 * @param a2
	 * @return
	 */
	public static float averageAngle(float a1, float a2){
		float naiveDiff = a1 - a2;
		if(naiveDiff > 180)
			a1 -= 360;
		else if(naiveDiff < -180)
			a2 -= 360;
		
		float avg = (a1 + a2) / 2;
		
		if(avg < 0)
			avg += 360;
		
		return avg;
		
	}
	
}
