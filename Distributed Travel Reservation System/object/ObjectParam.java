package object;

import java.util.List;
import java.util.ArrayList;
import java.util.Vector;
import java.io.*;

public class ObjectParam implements Serializable{
	
	
	
	int Id, Cid;
	ArrayList<Integer> flightNums = new ArrayList<Integer>();
    int flightPrice;
    int flightSeats;
    boolean Room;
    boolean Car;
    int price;
    int numRooms;
    int numCars;
    String location;
    String className;
    String methodName;

    
    public ObjectParam(){
    	
    }
    
	Vector arguments  = new Vector();
    public ObjectParam(Vector arguments, int id, int cid, int flightNum,
			int flightPrice, int flightSeats, boolean room, boolean car,
			boolean flight, int price, int numRooms, int numCars, 
			String location, String className, String methodName) {
		super();
		this.arguments = arguments;
		Id = id;
		Cid = cid;
		flightNums.add(flightNum);
		this.flightPrice = flightPrice;
		this.flightSeats = flightSeats;
		Room = room;
		Car = car;
		this.price = price;
		this.numRooms = numRooms;
		this.numCars = numCars;
		this.location = location;
		this.className = className;
		this.methodName = methodName;
	}
	
	
	 public Vector getArguments() {
		return arguments;
	}
	public void setArguments(Vector arguments) {
		this.arguments = arguments;
	}
	public int getId() {
		return Id;
	}
	public void setId(int id) {
		Id = id;
	}
	public int getCid() {
		return Cid;
	}
	public void setCid(int cid) {
		Cid = cid;
	}
	public Vector getFlightNums() {
		return new Vector(flightNums);
	}
	public int getLastFlight() {
		return flightNums.get(flightNums.size()-1);
	}
	public void addFlightNum(int flightNum) {
		this.flightNums.add(flightNum);
	}
	public void clearFlightNums() {
		this.flightNums.clear();
	}
	public int getFlightPrice() {
		return flightPrice;
	}
	public void setFlightPrice(int flightPrice) {
		this.flightPrice = flightPrice;
	}
	public int getFlightSeats() {
		return flightSeats;
	}
	public void setFlightSeats(int flightSeats) {
		this.flightSeats = flightSeats;
	}
	public boolean isRoom() {
		return Room;
	}
	public void setRoom(boolean room) {
		Room = room;
	}
	public boolean isCar() {
		return Car;
	}
	public void setCar(boolean car) {
		Car = car;
	}
	public boolean isFlight() {
		return !Car && !Room;
	}
	public int getPrice() {
		return price;
	}
	public void setPrice(int price) {
		this.price = price;
	}
	public int getNumRooms() {
		return numRooms;
	}
	public void setNumRooms(int numRooms) {
		this.numRooms = numRooms;
	}
	public int getNumCars() {
		return numCars;
	}
	public void setNumCars(int numCars) {
		this.numCars = numCars;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	
	 @Override
		public String toString() {
			return "ObjectParam [Id=" + Id + ", Cid=" + Cid + ", flightNums="
					+ flightNums.toArray().toString() + ", flightPrice=" + flightPrice + ", flightSeats="
					+ flightSeats + ", Room=" + Room + ", Car=" + Car + ", price="
					+ price + ", numRooms=" + numRooms + ", numCars=" + numCars
					+ ", location=" + location + ", className=" + className
					+ ", methodName=" + methodName + ", arguments=" + arguments
					+ "]";
		}
	
}
