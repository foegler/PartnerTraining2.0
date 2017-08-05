package PartnerTrainingDF2;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Simple class representing metadata about a package's pickup or
 * drop-off. Is meant to be read from a log line. 
 */
@DefaultCoder(AvroCoder.class)
public class PackageActivityInfo {
	private static final Logger LOG = LoggerFactory.getLogger(PackageActivityInfo.class);
	
	private boolean isArrival; // If true, is an arrival event; otherwise departing.
	private String location; // Two letter code for factory id or "CU" for customer counter.
	private Date time; // Time of the drop off or pickup activity.
	private int truckId; // ID of the truck doing the drop off or pick up. 0 if customer.
	private String packageId; // ID of the package in transit.

	public PackageActivityInfo() {}
	public PackageActivityInfo(boolean isArrival, String location, Date time, int truckId, String packageId) {
		this.isArrival = isArrival;
		this.location = location;
		this.time = time;
		this.truckId = truckId;
		this.packageId = packageId;
	}
	
	@Override
	public String toString() {
		return "PackageActivityInfo [isArrival=" + isArrival + ", location=" + location + ", packageId=" + packageId + "]";
	}

	public boolean isArrival() {
		return isArrival;
	}

	public String getLocation() {
		return location;
	}

	public Date getTime() {
		return time;
	}

	public int getTruckId() {
		return truckId;
	}

	public String getPackageId() {
		return packageId;
	}
	
	public void setArrival(boolean arrival) {
		isArrival = arrival;
	}
	public void setLocation(String loc) {
		location = loc;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public void setTruckId(int truckId) {
		this.truckId = truckId;
	}
	public void setPackageId(String packageId) {
		this.packageId = packageId;
	}

	// Return null if there is any error parsing.
	// Delimits line by the given delimeter.
	// Logline: "0, AN, 1467394122, 423, 372A3SZ4J98"
	public static PackageActivityInfo Parse(String logLine) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logLine.split(",");
			if (pieces.length != 5)
				return null;
			int isArrivalInt = Integer.parseInt(pieces[0].trim());
			if (isArrivalInt == 0) {
				pickup.isArrival = false;
			} else if (isArrivalInt == 1) {
				pickup.isArrival = true;
			} else {
				return null;
			}
			pickup.location = pieces[1].trim();
			pickup.time = new Date(Long.parseLong(pieces[2].trim()) * 1000);
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}
	
	// Parses the package log as if it was an older file (before
	// multiple locations). In this case all logs were departures
	// and the location was always from the one location "AR".
	public static PackageActivityInfo ParseOld(String logline) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logline.split(",");
			if (pieces.length != 3)
				return null;
			pickup.isArrival = false;
			pickup.location = "AR";
			pickup.time = new Date(Long.parseLong(pieces[2].trim()) * 1000);
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}
	
	static class ParseLine extends DoFn<String, PackageActivityInfo> {
        private Counter counter = Metrics.counter(ParseLine.class,
                "invalidLogLines");
        
        @ProcessElement
        public void processElement(ProcessContext c) {
  			String logLine = c.element();
  			PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
  			if (info == null) {
                counter.inc();
  			} else {
  				c.output(info);
  			}
  		}
  	}
	
    public static final List<String> MINI_LOG = Arrays.asList(
            "0, AN, 1467394122, 423, 372A3SZ4J98",
            "0, AN, 1467394122, 423##############", "404 - broken message",
            "0, AN, 1467394122, 423, 372A3SZ4J98");
}