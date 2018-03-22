package de.dietzm.utp.timanalysis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.util.log.Log;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.dietzm.utp.timanalysis.model.WeekdayAndHour;

@WebServlet(name = "weekdayAndHours", urlPatterns = { "/api/weekdayAndHours" })
public class WeekdayAndHoursService extends HttpServlet {

	private static final String SQL_TABLENAME = "aggregate_weekdayhours";;
	Connection conn;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		
		int weekday = getParameterAndCastToInt(request, "weekday"); 
		int hour = getParameterAndCastToInt(request, "hour");
		
		JsonArray result = querySQLTable(weekday, hour);
		
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

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");

		int createdEntrys = queryBigquery();
		response.getWriter().print("Crunshed " + createdEntrys + " entries");
		

	}
	
	public JsonArray querySQLTable(int weekday, int hour) throws ServletException {
		 
		String where = " 1 = 1 ";
		String project = " MT.segmentid ";
				
		if(weekday > -1) {
			where += " AND MT.weekday = " + weekday;
			project += ",MT.weekday ";
		} 
		
		if(hour > -1) {
			where += " AND MT.hour = " + hour;
			project += ",MT.hour ";
		} 
		
		
		String query = "SELECT IT.*, RS.start_lon, RS.start_lat, RS.end_lon, RS.end_lat ";
		query += " FROM (";
		query += " SELECT " + project + ", AVG(average_speed) AS average_speed FROM "+ SQL_TABLENAME + " AS MT";
		query += " WHERE " + where;
		query += " GROUP BY " + project + " ) AS IT";
		query += " LEFT OUTER JOIN road_segments AS RS ON RS.segmentid = IT.segmentid";
		JsonArray entries = new JsonArray();
		
		try {
			ResultSet rs = conn.createStatement().executeQuery(query);
			
			while(rs.next()) {
				JsonObject entry = new JsonObject();
				entry.addProperty("segmentid", rs.getInt("segmentid"));
				
				if(weekday > -1)
					entry.addProperty("weekday", rs.getInt("weekday"));
				
				if(hour > -1)
					entry.addProperty("hour", rs.getInt("hour"));
				
				entry.addProperty("average_speed", rs.getFloat("average_speed"));
				
				entry.addProperty("start_lon", rs.getFloat("start_lon"));
				entry.addProperty("start_lat", rs.getFloat("start_lat"));
				entry.addProperty("end_lon", rs.getFloat("end_lon"));
				entry.addProperty("end_lat", rs.getFloat("end_lat"));
				
				entries.add(entry);
			}
			
		} catch (SQLException e) {
			throw new ServletException("Error executing BQ Query", e);
		}
		
		return entries;
		
	}

	public int queryBigquery() throws ServletException {

		String tablename = "utp.traffic_data";

		String query = "SELECT ";
		query += "SEGMENTID AS segmentid, ";
		query += "EXTRACT(DAYOFWEEK FROM TIME) AS weekday, ";
		query += "EXTRACT(HOUR FROM TIME) AS hour, ";
		query += "-5  AS timezone, ";
		query += "COUNT(*) AS count, ";
		query += "AVG(SPEED) AS average_speed ";
		query += "FROM " + tablename + " ";
		query += "GROUP BY SEGMENTID, WEEKDAY, HOUR";

		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		QueryJobConfiguration queryRequest = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

		try {
			TableResult result = bigquery.query(queryRequest);
			ArrayList<WeekdayAndHour> buffer = new ArrayList<WeekdayAndHour>();		
			
			System.out.println("number of bq results" + result.getTotalRows());
			deleteTable();
			int cnt = 0;
			
			while (result != null) {
				
				for (List<FieldValue> row : result.iterateAll()) {
					WeekdayAndHour entry = new WeekdayAndHour();
					entry.setSegmentid((int) row.get(0).getLongValue());
					entry.setWeekday((int) row.get(1).getLongValue());
					entry.setHour((int) row.get(2).getLongValue());
					entry.setTimezone((int) row.get(3).getLongValue());
					entry.setCount((int) row.get(4).getLongValue());
					entry.setAverage_speed((int) row.get(5).getDoubleValue());
//					System.out.println("into buffer -> " +  entry.toString());
					buffer.add(entry);
					cnt++;
					
					if(buffer.size() >= 10000) {						
//						System.out.println("persist the buffer with size" + buffer.size());						
						insertToSQLTable(buffer);
						buffer = new ArrayList<WeekdayAndHour>();
					}					
				}
				
				Log.getLog().debug("next page at buffer size " + buffer.size() + " total size " + cnt);
				
				result = result.getNextPage();
			}
			
			insertToSQLTable(buffer);

			Log.getLog().debug("totally inserted " + cnt);
			
			return cnt;

		} catch (InterruptedException e) {
			throw new ServletException("Error executing BQ Query", e);
		} catch (SQLException e) {
			throw new ServletException("Error executing BQ Query", e);
		}
		
		
	}

	private void insertToSQLTable(ArrayList<WeekdayAndHour> objectList) throws SQLException {
		
		String insertStatement = "INSERT INTO "+SQL_TABLENAME+" (segmentid, weekday,hour, timezone, count, average_speed) VALUES (?,?,?,?,?,?)";
		insertStatement += " ON CONFLICT (segmentid, weekday, hour) DO NOTHING";
		PreparedStatement pstmt = conn.prepareStatement(insertStatement);
		
		for(int i = 0; i < objectList.size(); i++) {
			WeekdayAndHour entry = objectList.get(i);
			pstmt.setInt(1, entry.getSegmentid());
			pstmt.setInt(2, entry.getWeekday());
			pstmt.setInt(3, entry.getHour());
			pstmt.setInt(4, entry.getTimezone());
			pstmt.setInt(5, (int)entry.getCount());
			pstmt.setDouble(6, entry.getAverage_speed());
//			System.out.println("to db -> " +  entry.toString());
			pstmt.addBatch();
		}
		
		
		pstmt.executeBatch();	
		pstmt.clearBatch();
		
		System.out.println(objectList.size() + " batch successfully posted to SQL");
		
	}
	
	private void deleteTable() throws SQLException {
		
		String deleteStatement = "DELETE FROM "+SQL_TABLENAME+" ";
		conn.createStatement().executeUpdate(deleteStatement);
	
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

			initTable();

		} catch (ClassNotFoundException e) {
			throw new ServletException("Error loading JDBC Driver", e);
		} catch (SQLException e) {
			throw new ServletException("Unable to connect to PostGre", e);
		}
	}

	public void initTable() throws SQLException {

		
		String createTableSql = "CREATE TABLE IF NOT EXISTS " + SQL_TABLENAME + "( ";
		createTableSql += "segmentid 		INTEGER, ";
		createTableSql += "weekday	 		INTEGER, ";
		createTableSql += "hour				INTEGER, ";
		createTableSql += "timezone    		INTEGER, ";
		createTableSql += "count			INTEGER, ";
		createTableSql += "average_speed 	DOUBLE PRECISION, ";
		createTableSql += "PRIMARY KEY(segmentid, weekday, hour) ) ";

		conn.createStatement().executeUpdate(createTableSql);

	}

}