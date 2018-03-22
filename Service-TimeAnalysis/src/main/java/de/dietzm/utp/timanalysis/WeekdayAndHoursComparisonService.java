package de.dietzm.utp.timanalysis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonArray;

@WebServlet(name = "weekdayAndHoursComparison", urlPatterns = { "/api/weekdayAndHoursComparison" })
public class WeekdayAndHoursComparisonService extends HttpServlet {

	private static final String SQL_TABLENAME = "aggregate_weekdayhours";;
	Connection conn;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		
		int weekday1 = getParameterAndCastToInt(request, "weekday1"); 
		int hour1 = getParameterAndCastToInt(request, "hour1");
		int weekday2 = getParameterAndCastToInt(request, "weekday2"); 
		int hour2 = getParameterAndCastToInt(request, "hour2");
		
		JsonArray result = querySQLTable(weekday1, hour1, weekday2, hour2);
		
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(result.toString());
	}

	private int getParameterAndCastToInt(HttpServletRequest request, String name) {
		String param = request.getParameter(name);
		if(param != null && !param.equals("")) {
			return new Integer(param).intValue();
		}
		return -1;
	}

	
	public JsonArray querySQLTable(int weekday1, int hour1, int weekday2, int hour2) throws ServletException {
		 
		return new JsonArray();
//		String where1 = " 1 = 1 ";
//		String project1 = " MT.segmentid ";
//				
//		if(weekday1 > -1) {
//			where1 += " AND MT.weekday = " + weekday1;
//			project1 += ",MT.weekday ";
//		} 
//		
//		if(hour1 > -1) {
//			where1 += " AND MT.hour = " + hour2;
//			project1 += ",MT.hour ";
//		} 
//		
//		
//		String query = "SELECT IT.*, RS.start_lon, RS.start_lat, RS.end_lon, RS.end_lat ";
//		query += " FROM (";
//		query += " SELECT " + project1 + ", AVG(average_speed) AS average_speed FROM "+ SQL_TABLENAME + " AS MT";
//		query += " WHERE " + where1;
//		query += " GROUP BY " + project1 + " ) AS IT";
//		query += " LEFT OUTER JOIN road_segments AS RS ON RS.segmentid = IT.segmentid";
//		JsonArray entries = new JsonArray();
//		
//		try {
//			ResultSet rs = conn.createStatement().executeQuery(query);
//			
//			while(rs.next()) {
//				JsonObject entry = new JsonObject();
//				entry.addProperty("segmentid", rs.getInt("segmentid"));
//				
//				if(weekday > -1)
//					entry.addProperty("weekday", rs.getInt("weekday"));
//				
//				if(hour > -1)
//					entry.addProperty("hour", rs.getInt("hour"));
//				
//				entry.addProperty("average_speed", rs.getFloat("average_speed"));
//				
//				entry.addProperty("start_lon", rs.getFloat("start_lon"));
//				entry.addProperty("start_lat", rs.getFloat("start_lat"));
//				entry.addProperty("end_lon", rs.getFloat("end_lon"));
//				entry.addProperty("end_lat", rs.getFloat("end_lat"));
//				
//				entries.add(entry);
//			}
//			
//		} catch (SQLException e) {
//			throw new ServletException("Error executing BQ Query", e);
//		}
//		
//		return entries;
//		
	}

	
	@Override
	public void init() throws ServletException {
		String url;

		Properties properties = new Properties();
		try {
			properties.load(getServletContext().getResourceAsStream("/WEB-INF/classes/config.properties"));
			url = properties.getProperty("sqlUrl");
		} catch (IOException e) {
			log("no property", e); // Servlet Init should never fail.
			return;
		}

		log("connecting to: " + url);
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url);

		} catch (ClassNotFoundException e) {
			throw new ServletException("Error loading JDBC Driver", e);
		} catch (SQLException e) {
			throw new ServletException("Unable to connect to PostGre", e);
		}
	}

	

}