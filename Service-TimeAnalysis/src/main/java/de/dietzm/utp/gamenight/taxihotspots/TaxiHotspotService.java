package de.dietzm.utp.gamenight.taxihotspots;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@WebServlet(name = "taxihotspots", urlPatterns = { "/api/taxihotspots" })
public class TaxiHotspotService extends HttpServlet {

	private static final String SQL_TABLENAME = "taxi_pickup_hotspots";
	
	Connection conn;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		String orderBy = request.getParameter("order");
		if(orderBy == null || orderBy.equals("")) {
			orderBy = "pickupcount";
		}
		
		int limit = getParameterAndCastToInt(request, "limit", 30); 
		
		
		JsonArray result = querySQLTable(orderBy,limit);
		
		
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(result.toString());
		
	} 
	
	private int getParameterAndCastToInt(HttpServletRequest request, String name, int def) {
		String param = request.getParameter(name);
		if(param != null && !param.equals("")) {
			return new Integer(param).intValue();
		}
		return def;
	}
	

	public JsonArray querySQLTable(String orderBy, int limit) throws ServletException {
		
		String query = "SELECT * FROM " + SQL_TABLENAME + " ";
		query += " ORDER BY " + orderBy + " DESC LIMIT " + limit;
		JsonArray entries = new JsonArray();
		
		try {
			ResultSet rs = conn.createStatement().executeQuery(query);
			
			while(rs.next()) {
				JsonObject entry = new JsonObject();
	
				entry.addProperty("latitude", rs.getFloat("latitude"));
				entry.addProperty("longitude", rs.getFloat("longitude"));
				entry.addProperty("pickupcount", rs.getInt("pickupcount"));
				entry.addProperty("averagetripseconds", rs.getFloat("averagetripseconds"));
				entry.addProperty("averagetripmiles", rs.getFloat("averagetripmiles"));
				entry.addProperty("averagefare", rs.getFloat("averagefare"));
				
				entries.add(entry);
			}
			
		} catch (SQLException e) {
			throw new ServletException("Error executing SQL Query", e);
		}
		
		return entries;
		
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