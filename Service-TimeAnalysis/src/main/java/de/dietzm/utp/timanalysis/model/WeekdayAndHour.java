package de.dietzm.utp.timanalysis.model;

public class WeekdayAndHour {

	private int segmentid;
	private int weekday;
	private int hour;
	private int timezone;
	private long count;
	
	private double average_speed;
	public int getSegmentid() {
		return segmentid;
	}
	public void setSegmentid(int segmentid) {
		this.segmentid = segmentid;
	}
	public int getWeekday() {
		return weekday;
	}
	public void setWeekday(int weeday) {
		this.weekday = weeday;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public int getTimezone() {
		return timezone;
	}
	public void setTimezone(int timezone) {
		this.timezone = timezone;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public double getAverage_speed() {
		return average_speed;
	}
	public void setAverage_speed(double average_speed) {
		this.average_speed = average_speed;
	}
	
	public String toString() {
		return "(" + segmentid +"," + weekday+ "," + hour+ ") - " + count;
	}
	
	
}
