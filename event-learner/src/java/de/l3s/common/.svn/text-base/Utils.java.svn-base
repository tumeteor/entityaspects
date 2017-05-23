package de.l3s.common;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


import de.l3s.tools.BinningStrategy;
import de.l3s.tools.LocalDateBinner;
import de.l3s.tools.LocalDateRange;

public class Utils {	
    	
	public Connection conn = null;
	
	public int noOfAllQueries = 0;
	public int noOfAllURLs = 0;		
		
	public HashMap<Integer, String> QIDQueries = new HashMap<Integer, String>();		
	public HashMap<String, Integer> queryQIDs = new HashMap<String, Integer>();
	public HashMap<String, Integer> urlUIDs = new HashMap<String, Integer>();
	public HashMap<Integer, String> UIDUrls = new HashMap<Integer,String>();
	
	public HashMap<String, HashMap<String, HashMap<String, Integer>>> queryDateURLFreq = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
	public HashMap<String, HashMap<String, HashMap<String, Integer>>> urlDateQueryFreq = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();	

	public HashMap<String, HashMap<String, HashMap<String, Integer>>> queryDateURLAccFreq = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
	public HashMap<String, HashMap<String, HashMap<String, Integer>>> urlDateQueryAccFreq = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();	

	
	public void modGraph(String directory, String target){
		File explicit_dir = new File(directory);
		File[] listOfFiles = explicit_dir.listFiles();
		ArrayList<File> query_urls = new ArrayList<File>();
		ArrayList<File> pQuery_urls = new ArrayList<File>();
		ArrayList<File> gFiles  = new ArrayList<File>();
		for(File file : listOfFiles){
			if(file.getName().startsWith("pQuery-url") && file.getName().endsWith(".dat")){
				query_urls.add(file);
			}else if(file.getName().startsWith("query-url") && file.getName().endsWith(".dat")){
				 pQuery_urls.add(file);
			}else if(file.getName().startsWith("q-u")&& file.getName().endsWith(".dat")){
				gFiles.add(file);
			}
		}
		
		for(File gFile : gFiles){
			try {
				String suffix = gFile.getName().replace("q-u", "");
				System.out.println(suffix);
				List<String> gLines = Files.readLines(gFile, Charsets.UTF_8);
				File pQuery_url = null;
				File query_url = null;
				for(File pQuery_url_ : pQuery_urls){
					if(pQuery_url_.getName().contains(suffix)){
						pQuery_url = pQuery_url_;
						break;
					}
				}
				for(File query_url_ : query_urls){
					if(query_url_.getName().contains(suffix)){
						query_url = query_url_;
						break;
					}
				}
				List<String> pLines = Files.readLines(pQuery_url, Charsets.UTF_8);
				List<String> qLines = Files.readLines(query_url, Charsets.UTF_8);
				StringBuilder sb = new StringBuilder();
				for(int index = 0; index < qLines.size(); index ++){
					String[] qelements = qLines.get(index).split("\t");
					sb.append(gLines.get(index)).append("\t").append(qelements[3]).append("\t").append(qelements[4])
					  .append("\n");
				}
				for(int index = 0; index < pLines.size(); index ++){
					String[] pelements = pLines.get(index).split("\t");
					sb.append(gLines.get(qLines.size()+index)).append("\t").append(pelements[3]).append("\t").append(pelements[4])
					  .append("\n");
				}
				Files.write(sb.toString(), new File(target+"/q-u"+suffix),Charsets.UTF_8);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @param event time
	 * @param variance
	 * @return array of starting expanded date and
	 * ending expanded date
	 */
	public String[] getVariedDate(String eventDate, int variance){
		DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyyMMdd");

		LocalDate variedSDate;
		LocalDate variedEDate;
		
		//check eventDate is a period
		if (eventDate.contains("-")){
			String[] seDate = eventDate.split("\\-");
            LocalDate startDate = LocalDate.parse(seDate[0], dateFormat);
            LocalDate endDate = LocalDate.parse(seDate[1], dateFormat);
            //expand event period
            variedSDate = startDate.minusDays(variance);
            variedEDate = endDate.plusDays(variance);	
            return new String[]{variedSDate.toString(),variedEDate.toString()};		
		}else{
			//eventDate is a single date
			LocalDate eventLoDate = LocalDate.parse(eventDate, dateFormat);
			variedSDate = eventLoDate.minusDays(variance);
			variedEDate = eventLoDate.plusDays(variance);
			return new String[]{variedSDate.toString(),variedEDate.toString()};	
		}
		
	}

    
	/**
     * Method for shifting a date by a given offset
     *  
     * @param a date to shift, i.e. 'yyyymm'
     * @param an offset (in month)
     * @return a shifted date
     */
    public String shiftDateByOffset(String date, String dateFormat, int field, int offset) {
    	//String dateFormat = "yyyyMMdd";
    	
	    try {       		    	
    		Date inputDate = new SimpleDateFormat(dateFormat).parse(date);
    		Calendar inputDateCal = new GregorianCalendar(Locale.US);
    		inputDateCal.setTime(inputDate);
    		inputDateCal.add(field, offset);

    		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	    	String shiftedDate = sdf.format(inputDateCal.getTime());
			
			return shiftedDate;
        } catch (ParseException e) {
        	e.printStackTrace();
        	return null;
        }        
    }
        
	public String escapeSQL(String inputSQL) {
		if(inputSQL.contains("'")) {
			inputSQL = inputSQL.replace("'", "\\'");			
		}
		
		return inputSQL;
	}
	public Connection connectDB3306() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection conn = null;
		try {
			String userName = "kanhabua";
            String password = "zFQ8XDpjMDcsy47K";
            String url = "jdbc:mysql://db.l3s.uni-hannover.de:3306/";
            conn = DriverManager.getConnection(url, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println ("Cannot connect to database server");
		}
        
        return conn;
	}
	
	public Connection connectDB3308() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection conn = null;
		try {
			String userName = "kanhabua";
            String password = "5XUVbWfsGsMB9bct";
            String url = "jdbc:mysql://db.l3s.uni-hannover.de:3308/";
            conn = DriverManager.getConnection(url, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println ("Cannot connect to database server");
		}
        
        return conn;
	}
	
	public void closeDB(Connection conn) {		       
        if(conn != null) {
            try {
                conn.close ();
                System.out.println ("Database connection terminated");
            } catch(Exception e) { 
            	/* ignore close errors */ 
            }
        }
	}	
	
	public static void main (String args[]){
		Utils utils = new Utils();
		utils.modGraph(args[0], args[1]);	
	}
}
