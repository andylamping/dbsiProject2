package util;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import util.Condition;
import util.HeapFile;
import util.Record;
import compare.Comparer;

public class Query {

	public HeapFile heapFile;
	private String[] args;
	public ArrayList<ArrayList<Condition>> dummyRecord;
	private int argIndex;
	public ArrayList<String> conditionList = new ArrayList<String>();
	public ArrayList<String> projectionList = new ArrayList<String>();
	public ArrayList<Integer> projections = new ArrayList<Integer>();
	public ArrayList<Integer> matchingRecords;
	public ArrayList<Long> hashRecords;

	public Query(HeapFile inputHeap, String[] arguments) {
		this.heapFile = inputHeap;
		this.args = arguments;
	}

	public void processQuery (){
		// check if there is a query in the command line
		int hasQuery = this.hasQuery();
		// if not, program terminates
		if(hasQuery == 0){
			System.out.println("no query");
			return;
		}
		System.out.println("there is a query");
		this.dummyRecord = new ArrayList<ArrayList<Condition>>();
		this.projectionList = new ArrayList<String>();
		this.argIndex = 1;
		this.addConditions();
		this.argIndex = 1;
		this.addProjections();
		if (projectionList.size() != 0)
			this.projections = this.computeProjectionArray();
		this.findMatchingRecords();
		if(this.matchingRecords != null){
			Output output = new Output(this);
		}
		else if(this.hashRecords != null){
			Output output = new Output(this);
		}
		else{

		}
	}
	private int hasQuery() {
		// scan the arguments to see if there is a condition or projection
		// if there is one
		int x = 1;
		while(x < this.args.length){
			if(args[x].contains("-s") || args[x].contains("-p")){
				return 1;
			}
			x++;
		}
		return 0;
	}

	private void addConditions(){

		int argCount = 0;
		int multiCondition = 0;


		this.argIndex = 1;
		// traverse command line and add condition(s) to conditionList
		while(this.argIndex <= (args.length - 1)){
			// if argument contains an s, then we create a new condition,
			//and advance 3 spots in the index
			if(this.args[this.argIndex].contains("s")){
				System.out.println("there is a selection");
				argCount++;
				conditionList.add(this.args[this.argIndex]);
				int columnNumber = Integer.parseInt(this.args[this.argIndex].substring(2));
				// check that this is a valid column
				if(columnNumber > heapFile.numberOfFields || columnNumber == 0){
					System.out.println("Sorry. That column for query does not exist.");
					// come back to this
					return;
				}

				// if this isnt the first condition added, and this condition is equal to the previous
				// condition, then it is a multicondition. so we added it to the arraylist that already
				// exists for that column number
				if(argCount > 1 && this.args[this.argIndex].equals(this.args[this.argIndex - 3])){
					Condition condition = new Condition(this.args[this.argIndex], this.args[this.argIndex + 1], this.args[this.argIndex + 2], columnNumber);
					// multiList.add(condition);
					multiCondition++;
					if(multiCondition >= dummyRecord.size()){
						ArrayList<Condition> next = new ArrayList<Condition>();
						dummyRecord.add(next);
						dummyRecord.get(multiCondition).add(condition);
					}
					else{
						dummyRecord.get(multiCondition).add(condition);
					}
					this.argIndex = this.argIndex + 3;

				}
				// this is a totally new column to be queried
				else{
					multiCondition = 0;
					Condition condition = new Condition(this.args[this.argIndex], this.args[this.argIndex + 1], this.args[this.argIndex + 2], columnNumber);
					ArrayList<Condition> added = new ArrayList<Condition>();
					dummyRecord.add(added);
					dummyRecord.get(dummyRecord.size() - 1).add(condition);
					this.argIndex = this.argIndex + 3;
					System.out.println("added condition on new column");

				}
			}
			else
				argIndex++;

		}


	}


	private void addProjections() {
		this.argIndex = 1;
		// if argument contains a p, as in -p1, add this arg to projections and advance to next index
		while(this.argIndex <= (this.args.length - 1)){
			if(this.args[this.argIndex].contains("-p")){
				int columnNumber = Integer.parseInt(this.args[this.argIndex].substring(2));
				if(columnNumber > heapFile.numberOfFields || columnNumber == 0){
					System.out.println("Sorry. That column for projection does not exist.");
					return;
				}
				this.projectionList.add(this.args[this.argIndex]);
			}
			this.argIndex++;
		}
		System.out.println("there are " + this.projectionList.size() + " projections");
	}

	public void findMatchingRecords() {

		// this.dummyRecord now is an arraylist of arraylists
		// each index in dummyrecord corresponds to a different column
		// each column may have one or more conditions
		// for example: this.dummyRecord = [0][condition on column 1, different condition on column 1]
		// [1][condition on column 3]

		// first. we scan through the first condition in each list to find the column of that list
		// if this column has a hash index, we see if any of the conditions in that list
		// test for equality. if they do, we then use the index file to find the RIDs of the
		// value to be tested for equality.
		ArrayList<Long> allRIDs = new ArrayList<Long>();
		int hashes = 0;
		int x = 0;
		int advance = 1;
		System.out.println("dummy records has " + this.dummyRecord.size() + " size");
		while(x < this.dummyRecord.size()){
			// get column of this condition list
			int y = 0;
			int column = this.dummyRecord.get(x).get(y).column;
			System.out.println("there is a condition on column " + column);
			// check if there is a hash index on this column
			if(this.heapFile.indexExistsOnColumn(column)){

				while(y < this.dummyRecord.get(x).size()){
					// if current condition's parameter is equality, then we get the RIDs for the value
					String param = this.dummyRecord.get(x).get(y).operator;
					if(param.equals("=")){
						System.out.println("hash increase");
						// increase hashes
						hashes++;
						// get RID
						ArrayList<Long> equalityRIDs = this.heapFile.getListOfRidsForSelectionCondition(column, this.dummyRecord.get(x).get(y).value);
						System.out.println(equalityRIDs.size() + "returned RIDs size!!");
						// add RIDs to list
						int a = 0;
						while(a < equalityRIDs.size()){
							allRIDs.add(equalityRIDs.get(a));
							a++;
						}
						// remove this condition from this.dummyRecord
						this.dummyRecord.get(x).remove(y);
						// since we removed a condition, we dont want to skip the next condition
						// the slid down as a result of the delete
						advance = 0;
					}
					if(advance == 1){
						y++;
					}
					advance = 1;

				}
				// advance to next condition if there is one

			}
			x++;
		}

		// if 'hashes' > 1, then we can reduce the RID set by only keeping an RID
		// if it appears in the list 'hashes' amount of time
		if(hashes > 0){
			System.out.println("hashes > 0");
			if(hashes > 1){
				System.out.println("hashes > 1");
				ArrayList<Long> matchRIDs = new ArrayList<Long>();
				int e = 0;
				int matchesNeeded = hashes;

				while(e < allRIDs.size()){
					int f = e + 1;
					int matched = 0;
					int matches = 0;
					while(matched == 0 && f < allRIDs.size()){
						if(allRIDs.get(e).equals(allRIDs.get(f))){
							matches++;
						}
						if(matches == matchesNeeded){
							matchRIDs.add(allRIDs.get(e));
							matched = 1;
						}

						f++;
					}
					e++;
				}
				if(matchRIDs.size() == 0){
					// no matches in the file so we return null
					this.hashRecords = null;
					return;
				}
				// else switch allRIDs to the new set of matches
				allRIDs = matchRIDs;
			}
			// System.out.println(allRIDs.size() + "!!!");
			// if allRIDs.size() > 0, then we only want to compare the rest of the conditions with
			if(allRIDs.size() > 0){
				Comparer comparer1 = new Comparer();
				this.hashRecords = new ArrayList<Long>();


				try {

					RandomAccessFile heap = new RandomAccessFile(new File(this.heapFile.path), "r");
					Record dummyRec1 = new Record();
					int[] offsetList = this.heapFile.getOffsetList();
					int[] lengthList = this.heapFile.getListOfLengths();
					int z = 0;
					// go through each RID in allRIDs
					while(z < allRIDs.size()){
						// seek to first record in allRIDs
						// compare each condition in this.dummyRecord
						// seek to RID spot
						heap.seek(allRIDs.get(z));
						// reach record at this point in heap
						byte[] heapRec = new byte[this.heapFile.numberOfBytesPerRecord];
						heap.read(heapRec);

						/// compare with each condition list in this.dummyrecord
						int a = 0;
						int reject = 0;
						while(a < this.dummyRecord.size()){
							RandomAccessFile dum = new RandomAccessFile(new File("dummy"), "rw");
							// write to dummyrec1
							int[] compareList = new int[this.heapFile.schemaArray.length];
							compareList = dummyRec1.writeDummyFile(this.dummyRecord.get(a), compareList, this.heapFile);
							// read the record
							dum.seek(0);
							byte[] dumRec = new byte[this.heapFile.numberOfBytesPerRecord];
							dum.read(dumRec);


							Record results = new Record();
							int index = 0;
							int match;
							int condIndex1 = 0;
							while(index < compareList.length && reject == 0){
								int answer;
								if(compareList[index] == 1){
									answer = comparer1.compare_functions[this.heapFile.schemaArray[index]].compare(dumRec, offsetList[index], heapRec, offsetList[index],lengthList[index]);
									match = results.checkCompareResult(this.dummyRecord.get(a).get(condIndex1).operator, answer);
									if(match == 0){
										reject = 1;
									}
									condIndex1++;
								}

								index++;
							}
							dum.close();
							File dummy1 = new File("dummy");
							dummy1.delete();
							a++;
						}
						if(reject == 0){
							// match found, add to matchRecords list
							this.hashRecords.add(allRIDs.get(z));
						}

						z++;

					}
				} // end of try
				catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} // end of catchers
				System.out.println(this.hashRecords.size() + " EHY");
				return;
			}
		}

		else if (hashes == 0){
			System.out.println("no hashes");
			System.out.println("dummy rec size is " + this.dummyRecord.size());

			// end of try
			// if allRIDs.size() == 0 that must mean that we didnt have an index on any of the
			// of the columns that the query is conditioning
			// so we traverse the heapfile as usual

			// below is how we find matching records for a column that doesnt have an index
			Comparer comparer = new Comparer();
			this.matchingRecords = new ArrayList<Integer>();
			int m = 0;
			int[] offsetList = this.heapFile.getOffsetList();

			//int firstListCheck = 0;
			while(m < this.dummyRecord.size()){

				int[] compareList = new int[this.heapFile.schemaArray.length];
				Record dummyRec = new Record();

				compareList = dummyRec.writeDummyFile(this.dummyRecord.get(m), compareList, this.heapFile);


				// create RAF to read heapFile

				int[] lengthList = this.heapFile.getListOfLengths();
				RandomAccessFile dummy;
				try {
					dummy = new RandomAccessFile(new File("dummy"), "rw");
					RandomAccessFile raf1 = new RandomAccessFile(new File(this.heapFile.path), "r");

					int currentRecord = 0;


					while(currentRecord < this.heapFile.numberOfRecords){

						raf1.seek(this.heapFile.currentFileOffset + (this.heapFile.numberOfBytesPerRecord * currentRecord));
						byte[] heapRec = new byte[this.heapFile.numberOfBytesPerRecord];
						raf1.read(heapRec);

						dummy.seek(0);
						byte[] dumRec = new byte[this.heapFile.numberOfBytesPerRecord];
						dummy.read(dumRec);

						Record results = new Record();
						int index = 0;
						int reject = 0;
						int match;
						int condIndex1 = 0;
						while(index < compareList.length && reject == 0){
							int answer;
							if(compareList[index] == 1){
								answer = comparer.compare_functions[this.heapFile.schemaArray[index]].compare(dumRec, offsetList[index], heapRec, offsetList[index],lengthList[index]);
								match = results.checkCompareResult(this.dummyRecord.get(m).get(condIndex1).operator, answer);
								if(match == 0){
									reject = 1;
								}
								condIndex1++;
							}

							index++;
						}

						if(reject == 0){
							// match found, add to matchRecords list
							this.matchingRecords.add(currentRecord);
							System.out.println("match found in no hashes");
						}

						currentRecord++;
					} // end of scanning all records

					dummy.close();
					File dummy1 = new File("dummy");
					dummy1.delete();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} // end of catchers
				m++;
			} // end of m > 0 loop
			System.out.println("done scanning records");
			m = this.dummyRecord.size();
			ArrayList<Integer> matchRecs = new ArrayList<Integer>();
			int e = 0;
			int matchesNeeded = m - 1;
			if(m == 1){
				System.out.println(this.matchingRecords.size());
				return;
			}
			while(e < this.matchingRecords.size()){
				int f = e + 1;
				int matched = 0;
				int matches = 0;
				while(matched == 0 && f < this.matchingRecords.size()){
					if(this.matchingRecords.get(e).equals(this.matchingRecords.get(f))){
						matches++;
					}
					if(matches == matchesNeeded){
						System.out.println("found real match");
						matchRecs.add(this.matchingRecords.get(e));
						matched = 1;
					}

					f++;
				}
				e++;
			}
			if(matchRecs.size() == 0){
				this.matchingRecords = null;
				return;
			}
			this.matchingRecords = matchRecs;
			return;
		}
	}

	public ArrayList<Integer> computeProjectionArray(){
		ArrayList<Integer> projections = new ArrayList<Integer>();
		for (String s:this.projectionList){
			projections.add(Integer.parseInt(s.substring(2)) - 1);
		}
		System.out.println("projections size in CPA is " + projections.size());
		return projections;
	}
	public void findMatchingRecords2(){
		String currentRecord, schema;
		long RID;
		CSVFile csvTarget = new CSVFile("example_output.acsv");
		/* If no selection conditions then we must fetch all the records. */
		if (conditionList.size() == 0){

			schema = this.heapFile.schema;
			schema = projectData(schema);
			// Write schema to CSVFile
			csvTarget.writeDataToFile(schema);
			for (int i = 0 ; i<this.heapFile.numberOfRecords; i++){
				RID = heapFile.currentFileOffset + (i*heapFile.numberOfBytesPerRecord);
				currentRecord = heapFile.getRecordByRIDFromHeapFile(RID);
				// project data
				currentRecord = projectData(currentRecord);

				// Write Record to CSV File.
				csvTarget.writeDataToFile(currentRecord);
			}
		}

		/* If selection conditions exist, we need to fetch the appropriate records */
		else{
			// INSERT THE SELECTION CODE HERE FROM FINDMATCHINGRECORDS()
		}
	}

	/*
	 * Writes the Record numbers in this.matchingRecords ,
	 * after Projections to the file, example_output.acsv.
	 */
	public void writeSelectedDataAfterProjections (){
		String schema = "", currentRecord = "";
		long RID;
		CSVFile csvTarget = new CSVFile("example_output.acsv");
		
		schema = projectData(this.heapFile.schema);
		csvTarget.writeDataToFile(schema);
		
		for (Integer i:matchingRecords){
			RID = heapFile.currentFileOffset + (i*heapFile.numberOfBytesPerRecord);
			currentRecord = heapFile.getRecordByRIDFromHeapFile(RID);
			//project data 
			currentRecord = projectData(currentRecord);
			
			//Write the Record to CSV File
			csvTarget.writeDataToFile(currentRecord);
		}
	}
	
	public String projectData (String data){
		if (projectionList.size() != 0){
			String []dataElements = data.split(",");
			data = "";
			for (Integer j : projections){
				data += dataElements[j]+",";
			}
			data = data.substring(0,data.length()-1);
		}
		return data;
	}

}
