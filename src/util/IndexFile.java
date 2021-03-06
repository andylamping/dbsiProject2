package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import config.Config;

public class IndexFile {

	private String path;
	private String overFlowPath;
	public Integer nextPointer;
	private String dataType;
	private Integer columnLength;
	private Integer headerLength = 20;
	private Integer numberOfBuckets = 4;
	public long offsetNumberOfBuckets;
	private Integer round;
	private Integer splitting = 0;
	private Integer numberOfEntriesInBucket = Bucket.numberOfEntriesInBucket;
	public OverflowFile oFile; 

	// Intermediate Offset values
	public long offsetHeaderLength = 0;
	public long offsetColumnLength ;
	public long offsetNextPtr ;
	public long offsetRound;
	public long offsetEndOfHeader;
	public long currentFileOffset;

	public IndexFile (String path, String overflowPath, String datatype){
		this.path = path;
		this.round = 1;
		this.overFlowPath = overflowPath;
		this.oFile = new OverflowFile(this.overFlowPath);
		this.oFile.dataType = this.dataType;
		this.nextPointer = 0;
		this.dataType = datatype;
		this.columnLength = Integer.parseInt(datatype.substring(1));
		this.oFile.dataType = datatype;
		
		if (new File(path).exists())
			getHeaderInformationFromFile();
	}

	public Integer sizeOfBucket(){
		/* 
		// * Integer - maxSize + currentSize + Long - OffsetPointer + size of the 2D Object Data array 
		 * Integer - maxSize + currentSize + number of Buckets +Long - OffsetPointer + size of the 2D Object Data array
		 */
		//	return (4 + 4 + 8 +(this.numberOfEntriesInBucket * (this.columnLength + 8)));
		return (4 + 4 + 4 +8 +(this.numberOfEntriesInBucket * (this.columnLength + 8)));
	}

	public void writeHeaderInformationToFile (){
		/*
		 * Write header information to the Index File
		 * Header Length -  always 12 bytes 	 - 	length 4 bytes (since we store Integer value)
		 * Column Length -	depends on the value - 	length 4 bytes (since we store Integer value)
		 * next Pointer  -	depends on the value - 	length 4 bytes (since we store Integer value)
		 */
	//s	System.out.println("Writing headerinformation to index file");
		File f = new File(this.path);
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");

			raf.write(Helper.toByta(this.headerLength));

			this.offsetColumnLength = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.columnLength));

		//	System.out.println("writing pointer!!" + this.nextPointer);
			this.offsetNextPtr = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.nextPointer));

			this.offsetNumberOfBuckets = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.numberOfBuckets));

			this.offsetRound = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.round));

			this.offsetEndOfHeader = this.currentFileOffset = raf.getFilePointer();

			raf.close();
		} catch (FileNotFoundException e) {
			System.out.println("IndexFile - writing header information to file - " +
					"File Not Found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getHeaderInformationFromFile(){
		File f = new File(this.path);

		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");
			byte [] b = new byte [4];

			//Read Header Length
			raf.seek(0);
			raf.read(b, 0, 4);
			this.headerLength = Helper.toInt(b);

			//Read Column Length
			this.offsetColumnLength = this.currentFileOffset = raf.getFilePointer();
			raf.read(b,0,4);
			this.columnLength = Helper.toInt(b);

			//Read Next Pointer
			this.offsetNextPtr = this.currentFileOffset = raf.getFilePointer();
			raf.read(b,0,4);
			this.nextPointer = Helper.toInt(b);
			//Read Number of Buckets
			this.offsetNumberOfBuckets = this.currentFileOffset = raf.getFilePointer();
			raf.read(b,0,4);
			this.numberOfBuckets = Helper.toInt(b);

			//Read round number
			this.offsetRound = this.currentFileOffset = raf.getFilePointer();
			raf.read(b,0,4);
			this.round = Helper.toInt(b);

			this.offsetEndOfHeader = this.currentFileOffset = raf.getFilePointer();

			raf.close();

		} catch (FileNotFoundException e) {
			System.out.println("IndexFile - Get Header Info from File - File not Found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	/*
	 * TODO 
	 * Accept Data, get hashcode
	 */
	public void writeToIndexFile(Object data, long ptr){
		Integer destinationBucketNumber = this.getHash( data);
	//f	System.out.println("Inserting " + data + " into bucket:" + destinationBucketNumber + "at ptr " + ptr);
		insertIntoDestinationBucket(destinationBucketNumber, data, ptr);
	}


	private void insertIntoDestinationBucket(Integer destinationBucketNumber,
			Object data, long ptr) {

		/*
		 * Goto nth bucket using the formula
		 * 
		 * READ the entire bucket into memory, check if space exists.
		 * if yes, then write to memory
		 * else,
		 * 		READ the overflow bucket into memory
		 * 		check if space exists.
		 */
		Long destinationOffset = this.headerLength + (long) ((destinationBucketNumber)*sizeOfBucket());

		Bucket d = new Bucket(this.numberOfEntriesInBucket, (long) -1);
		d = d.readBucketFromFile(path, destinationOffset, dataType);

		long overflowBucketStartAddress, lastOverflowBucketStartAddress = 0;
		boolean writtenToBucket = false;
		boolean reuse = false;
		if (d.writeInfoToBucket(data, ptr)){
//			if (Config.DEBUG) System.out.println("Successfully entered into the same bucket");
			writtenToBucket = true;
		//	System.out.println(destinationBucketNumber + "  " + d);	
			d.writeBucketToFile(path, destinationOffset, dataType);
		}
		else {
	//		System.out.println(d);
		//	if (Config.DEBUG) System.out.println("Overflow has occured!!!");
			Bucket currentBucket = d;
			Bucket overflowBucket= new Bucket(numberOfEntriesInBucket, (long)-1) ;
			long currentBucketStartAddress = destinationOffset;
			//	System.out.println("here");
			/*
			 * Iterate to empty bucket.
			 * Assumption - all Buckets are filled to the max.
			 */
			Iterate:
				while ((overflowBucketStartAddress = currentBucket.getOverflowOffset()) != -1){
					overflowBucket = overflowBucket.readBucketFromFile(overFlowPath, overflowBucketStartAddress, dataType);
					//	System.out.println("here!!");
					if (overflowBucket.writeInfoToBucket(data, ptr)){
		//				if (Config.DEBUG) System.out.println("Data entered to overflow bucket");
						writtenToBucket = true;
						overflowBucket.writeBucketToFile(overFlowPath, overflowBucketStartAddress, dataType);
	//					if (Config.DEBUG) System.out.println("Inserted into pre-existing bucket " + overflowBucket);

						if(this.splitting == 0){
			//				System.out.println("A split must occur.");
							this.split();}
						break Iterate;
					}
					currentBucket = overflowBucket;
			//		System.out.println("STUCK");
					currentBucketStartAddress = overflowBucketStartAddress;
				}

			long newOverflowBucketStartAddress;
			if (!writtenToBucket){
				overflowBucket = new Bucket(numberOfEntriesInBucket, (long)-1);
				if ((newOverflowBucketStartAddress =  this.oFile.getBucketFromFreeList()) == -1){
					File f = new File(overFlowPath);
					newOverflowBucketStartAddress = f.length();
					reuse = false;
				}else{
					reuse = true;
				}

				if (reuse)
					overflowBucket = overflowBucket.readBucketFromFile(overFlowPath, newOverflowBucketStartAddress, dataType);
				else
					overflowBucket = new Bucket(numberOfEntriesInBucket, (long)-1);

				overflowBucket.writeData();
				overflowBucket.writeInfoToBucket(data, ptr);

				overflowBucket.writeBucketToFile(overFlowPath, newOverflowBucketStartAddress, dataType);
	//			if (Config.DEBUG) System.out.println("Inserted into new bucket " + overflowBucket);

				if (currentBucket == d){
					currentBucket.setOverflowOffset(newOverflowBucketStartAddress);
					currentBucket.setNumberOfOverflowBuckets(currentBucket.getNumberOfOverflowBuckets()+1);
					currentBucket.writeBucketToFile(path, currentBucketStartAddress, dataType);
			//		System.out.println("A split must occur!");
					if(this.splitting == 0){
			//			System.out.println("A split must occur.");
						this.split();}
				}else{
					d.setNumberOfOverflowBuckets(d.getNumberOfOverflowBuckets() +1);
					d.writeBucketToFile(path, destinationOffset, dataType); 
					currentBucket.setOverflowOffset(newOverflowBucketStartAddress);
					currentBucket.writeBucketToFile(overFlowPath, currentBucketStartAddress, dataType);
				//	System.out.println("A split must occur!!");
					if(this.splitting == 0){
				//		System.out.println("A split must occur.");
						this.split();}
				}
			}
		}


	}

	public Integer getHash(Object data){
		/*
		 * Compute the hash value of the data.
		 */

		// TODO divide hashcode with the appropriate
		// function - so that the value lies within the correct 
		// set of buckets.
		/**
		Integer b = data.hashCode() % this.numberOfBuckets;
		if (b < this.nextPointer){
			System.out.println("WEIRD!!!!!!!!");
			b = data.hashCode() % (2*this.numberOfBuckets);
		}
		return b; 
		 **/
		//	System.out.println(data.getClass());

		String str = "hey";
		if(str.getClass() == data.getClass()){
			//		System.out.println("STRING");
			str = data.toString();
			str = str.toLowerCase();
			Integer b = Math.abs(str.hashCode()) % this.numberOfBuckets;
			if(b < this.nextPointer)
				b = Math.abs(str.hashCode()) % (2 * this.numberOfBuckets);
			//	System.out.println(str + " !!! " + str.hashCode() + "  " + b);
			return b;
		}

		Integer b = Math.abs(data.toString().hashCode()) % this.numberOfBuckets;
		if(b < this.nextPointer)
			b = Math.abs(data.toString().hashCode()) % (2 * this.numberOfBuckets);
		//		System.out.println(data + " !!! " + data.hashCode() + "  " + b);
		return b;
	}

	/*
	 * Write initial buckets to the 
	 * Index file
	 */
	public void writeInitialBucketsToFile (){

		Bucket initial ;
		this.oFile.dataType = this.dataType;
		long offsetForNewBucket = this.currentFileOffset;
	//	System.out.println(this.dataType);
		for (int i = 0; i< this.numberOfBuckets; i++){
			initial = new Bucket(numberOfEntriesInBucket, (long)-1);
			initial.writeData();
			this.oFile.dataType = this.dataType;
			initial.writeBucketToFile(this.path, offsetForNewBucket, this.dataType);
			offsetForNewBucket += sizeOfBucket();
		}
	}
	public void split(){
		this.splitting = 1;

		// increase number of buckets in index file
		Bucket freshBucket = new Bucket(numberOfEntriesInBucket, (long) -1);
		freshBucket.writeData();
		freshBucket.writeBucketToFile(this.path, new File(this.path).length(), this.dataType);

		// set the offset for the current this.nextPointer bucket
		long offsetSplit = this.headerLength + this.nextPointer * sizeOfBucket();
		// create new bucket to copy that bucket
		Bucket splitBucket = new Bucket(numberOfEntriesInBucket, (long) -1);
		// copy that bucket to splitBucket
		splitBucket = splitBucket.readBucketFromFile(this.path, offsetSplit, this.dataType);
		//	System.out.println(" bucket to be split " + splitBucket.toString());
		int index = 0;
		// arrayList to store objects of the bucket and its overflow bucket
		ArrayList<Object> currentContents = new ArrayList<Object>();

		// get all data items from index bucket
		while ( index < splitBucket.getCurrentSize()){
			Object pluck = splitBucket.data[index][0];
			Object ptr = splitBucket.data[index][1];
			currentContents.add(pluck);
			currentContents.add(ptr);
			//	System.out.println(pluck);
			index++;

		}


		// get al

		//		int overFlowBucket1 = 0;
		//		int x = splitBucket.getNumberOfOverflowBuckets();
		//		//	System.out.println("OVERFLOWWWWSS" + splitBucket.getNumberOfOverflowBuckets());
		//		// traverse each overflow bucket
		//		while(overFlowBucket1 < x){
		//			System.out.println("enter" + splitBucket.getNumberOfOverflowBuckets());
		//			Bucket overflowBucket = splitBucket.readBucketFromFile(this.overFlowPath, splitBucket.getOverflowOffset() + (overFlowBucket1 * sizeOfBucket()), this.dataType);
		//			if(this.nextPointer == 3)
		//				System.out.println(overflowBucket);
		//
		//			index = 0;
		//
		//			// add all elements from the overflow bucket
		//			while ( index < overflowBucket.getCurrentSize()){
		//				Object pluck = overflowBucket.data[index][0];
		//				Object ptr = overflowBucket.data[index][1];
		//				currentContents.add(pluck);
		//				currentContents.add(ptr);
		//				System.out.println("Overflow item " + pluck);
		//				index++;
		//			}
		//
		//			// reset this overflow bucket
		//
		//			overFlowBucket1++;
		//		}
		//
		//		overFlowBucket1 = 0;
		//		while (overFlowBucket1 < x){
		//			splitBucket.resetBucket(this.overFlowPath, splitBucket.getOverflowOffset() - overFlowBucket1 * sizeOfBucket(), this.dataType);
		//			overFlowBucket1++;
		//		}
		Bucket overFlowBucket = splitBucket;
		long overFlowBucketOffsetAddress = 0, newOverFlowBucketOffsetAddress;

		while ((overFlowBucketOffsetAddress = overFlowBucket.getOverflowOffset()) != -1){
			// Add all the data for each overflowBucket to currentContents 
			// and reset the bucket
			overFlowBucket = overFlowBucket.readBucketFromFile(overFlowPath, overFlowBucketOffsetAddress, dataType);

			index = 0;
			while (index < overFlowBucket.getCurrentSize()){
				Object pluck = overFlowBucket.data[index][0];
				Object ptr = overFlowBucket.data[index][1];
				currentContents.add(pluck);
				currentContents.add(ptr);
	//			if(Config.DEBUG) System.out.println("Overflow item " + pluck);
				index++;
			}
			newOverFlowBucketOffsetAddress = overFlowBucketOffsetAddress;
			overFlowBucket.resetBucket(this.overFlowPath, overFlowBucketOffsetAddress, dataType);
			this.oFile.addBucketToFreeList(newOverFlowBucketOffsetAddress);

		}

		// all contents of bucket to be split and its overflow buckets now in currentContents
		splitBucket.resetBucket(this.path, this.headerLength + (long) this.nextPointer * sizeOfBucket(), this.dataType);
		index = 0;
//		System.out.println("THIS ROUND IS" + this.round);
	//	System.out.println("Rehashing bucket ---- NEXT POINTER IS " + this.nextPointer);
		this.nextPointer++;
	//	if (Config.DEBUG) System.out.println("NEXT POINTER HAS BEEN INCREMENTED");
		this.writeHeaderInformationToFile();
		//	System.out.println(this.nextPointer);
		while(index < currentContents.size()){
	//		System.out.println("Overflow!" + index);
			Object data = currentContents.get(index);
			Object ptr = currentContents.get(index + 1);
			int hash = getHash(data);
	//		System.out.println("New hash " + hash);
			this.writeToIndexFile(data, Long.parseLong(ptr.toString()));
			index = index + 2;	
		}


		//System.out.println("THIS ROUND IS" + this.round);
	//	System.out.println("NP = " + this.nextPointer + ". NB = " + this.numberOfBuckets + ". R = " + this.round);
		if(this.nextPointer == this.numberOfBuckets * (this.round)){
	//		System.out.println("******************** " + this.nextPointer );
			this.nextPointer = 0;
		//	this.numberOfBuckets = 2 * this.numberOfBuckets;
	//		System.out.println("num buck " + this.numberOfBuckets);
	//		System.out.println(this.numberOfBuckets * (this.round + 1) - 1 + "aaaaa");
			
			this.round++;
		}
		
		this.writeHeaderInformationToFile();
		this.splitting = 0;
	}


	/* Getter and Setters */
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getOverFlowPath() {
		return overFlowPath;
	}
	public void setOverFlowPath(String overFlowPath) {
		this.overFlowPath = overFlowPath;
	}
	public Integer getNextPointer() {
		return nextPointer;
	}
	public void setNextPointer(Integer nextPointer) {
		this.nextPointer = nextPointer;
	}
	public Integer getColumnLength() {
		return columnLength;
	}
	public void setColumnLength(Integer columnLength) {
		this.columnLength = columnLength;
	}
	public Integer getHeaderLength() {
		return headerLength;
	}
	public void setHeaderLength(Integer headerLength) {
		this.headerLength = headerLength;
	}

	public ArrayList<Long> getListOfRIDsForColumnValue(Object value) {
		// Calculate the bucket where we need to look 
		// for RIDs
		this.getHeaderInformationFromFile();
//		System.out.println(this.nextPointer + "NP");
		Integer bucketToBeSearched = getHash(value);
	//	System.out.println(bucketToBeSearched + " search bucket");
	//f	System.out.println(value + " value");
		Long bucketToBeSearchedOffset = this.headerLength + (long) ((bucketToBeSearched)*sizeOfBucket());

		ArrayList<Long> retValues = new ArrayList<Long>();

		Bucket search = new Bucket(numberOfEntriesInBucket, (long)-1);
		search = search.readBucketFromFile(path, bucketToBeSearchedOffset, dataType);
	//	if(search == null){
	//		System.out.println("read null bucket");
	//	}
	//	System.out.println(search.getCurrentSize());
		do{ // Read the Index bucket and all the overflow buckets.
			for (int i = 0 ; i <search.getCurrentSize() ; i++){
				// for each bucket - read all the data values.
		//		System.out.println("foudn value " + search.data[i][0]);
				//		System.out.println(search.data[i][0].getClass());
				if (search.data[i][0].toString().toLowerCase().equals(value.toString().toLowerCase())){

					// If value in the data array matches 
					// the value that we are searching 
					// add the RID to the list.
					retValues.add(Long.parseLong(search.data[i][1].toString()));
				}
			}
			// Read the next Overflow bucket into memory
		//	System.out.println(search);
			search = search.readBucketFromFile(overFlowPath, search.getOverflowOffset(), dataType);
		}while (search != null);


		return retValues;
	}


}



