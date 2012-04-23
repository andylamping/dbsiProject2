package util;

import helper.Helper;

import java.io.File;
import java.io.RandomAccessFile;

import config.Config;

public class OverflowFile {

	private String path;
	private long freeBucketList = -1;
	public long currentFileOffset;
	public String dataType;

	


	public OverflowFile(String overFlowPath) {
		this.path = overFlowPath;
		this.currentFileOffset = 0;
		this.freeBucketList = -1;
		
		File f = new File(path);
		if (f.exists())
			this.getHeaderInformationFromFile();
		else 
			writeHeaderInformationToFile();
	}

	public long writeNewBucketToFile(Bucket b){

		long startAddressOfBucket = this.currentFileOffset;

		return startAddressOfBucket;
	}
	
	public void getHeaderInformationFromFile(){
		try {
			RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(0);
			byte [] b = new byte[8];
			raf.read(b,0,8);
			this.setFreeBucketList(Helper.toLong(b));
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void writeHeaderInformationToFile(){
		try{
			RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(0);
			raf.write(Helper.toByta(this.freeBucketList));
			raf.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	

	public long getBucketFromFreeList (){
		getHeaderInformationFromFile();
		if (this.freeBucketList == -1){
			if (Config.DEBUG) System.out.println("No Free Bucket");
			return -1;
		}
		else {
			long returnValue = this.freeBucketList;
			Bucket b = new Bucket(Bucket.numberOfEntriesInBucket, (long) -1);
			b = b.readBucketFromFile(path, returnValue, dataType);
			this.freeBucketList = b.getOverflowOffset();
			if (Config.DEBUG) System.out.println("Free Bucket returned");
			return returnValue;
		}
	}
	
	public void addBucketToFreeList(long ptr){
		Bucket b = new Bucket(Bucket.numberOfEntriesInBucket, (long) -1);
		b.writeData();
		b = b.readBucketFromFile(path, ptr, dataType);
		b.setOverflowOffset(this.freeBucketList);
		this.freeBucketList = ptr;
		this.writeHeaderInformationToFile();
		if (Config.DEBUG) System.out.println("Bucket added to FreeList");
	}

	/* Getter and Setters */
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getFreeBucketList() {
		return freeBucketList;
	}

	public void setFreeBucketList(long freeBucketList) {
		this.freeBucketList = freeBucketList;
	}

}
