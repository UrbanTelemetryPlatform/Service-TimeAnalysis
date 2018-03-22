package de.dietzm.utp.gamenight;

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

import de.dietzm.utp.timanalysis.model.GamenightModel;

@WebServlet(name = "gamenightduring", urlPatterns = { "/api/duringGameNight" })
public class GameNightDuringService extends HttpServlet {

	private static final String SQL_TABLENAME = "analysis_during_gamenight";
	private static final String BQ_TABLE_NAME = "[utp-md:utp_view.gamenight_during_difference]";
	
	Connection conn;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		JsonArray result = querySQLTable();
		
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(result.toString());
		 
	}

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");

		int createdEntrys = queryBQAndTransferToSQL();
		response.getWriter().print("Crunshed " + createdEntrys + " entries");
	}
	 
	public JsonArray querySQLTable() throws ServletException {
		
		String query = "SELECT MT.*, RS.start_lon, RS.start_lat, RS.end_lon, RS.end_lat ";
		query += " FROM " + SQL_TABLENAME + " AS MT";
		query += " LEFT OUTER JOIN road_segments AS RS ON RS.segmentid = MT.segmentid";
		JsonArray entries = new JsonArray();
		
		try {
			ResultSet rs = conn.createStatement().executeQuery(query);
			
			while(rs.next()) {
				JsonObject entry = new JsonObject();
				entry.addProperty("segmentid", rs.getInt(1));
				entry.addProperty("speed", rs.getFloat(2));
				entry.addProperty("speed_ref", rs.getFloat(3));
				entry.addProperty("speed_difference", rs.getFloat(4));
				
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

	public int queryBQAndTransferToSQL() throws ServletException {
		
		String query = "SELECT * FROM " + BQ_TABLE_NAME + " ";

		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		QueryJobConfiguration queryRequest = QueryJobConfiguration.newBuilder(query).setUseLegacySql(true).build();

		try {
			TableResult result = bigquery.query(queryRequest);
			ArrayList<GamenightModel> buffer = new ArrayList<GamenightModel>();		
			
			System.out.println("number of bq results" + result.getTotalRows());
			deleteTable();
			int cnt = 0;
			
			while (result != null) {
				
				for (List<FieldValue> row : result.iterateAll()) {
					GamenightModel entry = new GamenightModel();
					entry.setSegmentid((int) row.get(0).getLongValue());
					entry.setSpeed(row.get(1).getDoubleValue());
					entry.setSpeed_ref(row.get(2).getDoubleValue());
					entry.setSpeed_difference(row.get(3).getDoubleValue());
					
					buffer.add(entry);
					cnt++;
					
					if(buffer.size() >= 10000) {						
//						System.out.println("persist the buffer with size" + buffer.size());						
						insertToSQLTable(buffer);
						buffer = new ArrayList<GamenightModel>();
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

	private void insertToSQLTable(ArrayList<GamenightModel> objectList) throws SQLException {
		
		String insertStatement = "INSERT INTO "+SQL_TABLENAME+" (segmentid, speed, speed_ref, speed_difference) VALUES (?,?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(insertStatement);
		
		for(int i = 0; i < objectList.size(); i++) {
			GamenightModel entry = objectList.get(i);
			pstmt.setInt(1, entry.getSegmentid());
			pstmt.setDouble(2, entry.getSpeed());
			pstmt.setDouble(3, entry.getSpeed_ref());
			pstmt.setDouble(4, entry.getSpeed_difference());			
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
	
	public void initTable() throws SQLException {

		String createTableSql = "CREATE TABLE IF NOT EXISTS " + SQL_TABLENAME + "( ";
		createTableSql += "segmentid 		INTEGER PRIMARY KEY, ";
		createTableSql += "speed	 		DOUBLE PRECISION, ";
		createTableSql += "speed_ref		DOUBLE PRECISION, ";
		createTableSql += "speed_difference	DOUBLE PRECISION ";
		createTableSql += ") ";

		conn.createStatement().executeUpdate(createTableSql);

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

	

}