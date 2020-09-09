//package ReadData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;


public class DataProcess {
	
	public static void main(String [] args) {
		long startTime = System.currentTimeMillis();
		
		try {
			Queue <data> data = new ConcurrentLinkedQueue<data>();			
			Map<String,LinkedList<data>> map = new HashMap<String, LinkedList<data>>();
			
			readTask task1 = new readTask(data);
			processTask task2 = new processTask(data,map);		
			outTask task3 = new outTask(map);
			
			Thread thread1 = new Thread(task1);
			Thread thread2 = new Thread(task2);			
			
			thread1.start();
			thread2.start();
			thread1.join();
			thread2.join();
			
			task3.run_outTask();
		}catch (Exception e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
	}	
}

class readTask implements Runnable{
	public static boolean read_done;
	public readTask(Queue <data> queue ) {
		this.queue = queue;		
	}
	public void run() {
		try {
			String filename = "F:\\Java\\Test\\src\\ReadData\\data.txt";
			File file = new File(filename);
			BufferedReader reader = null;		
			String strTemp;
			reader = new BufferedReader(new FileReader(file));
			while((strTemp = reader.readLine()) != null) {
				String []item = strTemp.split(",");	
				queue.add(new data(item[0],Integer.parseInt(item[1]),Integer.parseInt(item[2]),Integer.parseInt(item[3]),Float.parseFloat(item[4]),Float.parseFloat(item[5]),Float.parseFloat(item[6]),Float.parseFloat(item[7]),Float.parseFloat(item[8]),Float.parseFloat(item[9]),Float.parseFloat(item[10]),Float.parseFloat(item[11]),
	            		  Float.parseFloat(item[12]),Float.parseFloat(item[13]),Float.parseFloat(item[14]),Float.parseFloat(item[15]),Float.parseFloat(item[16]),Float.parseFloat(item[17]),Float.parseFloat(item[18]),Integer.parseInt(item[19]),Integer.parseInt(item[20]),Integer.parseInt(item[21]),Integer.parseInt(item[22]),
	            		  Integer.parseInt(item[23]),Integer.parseInt(item[24]),Integer.parseInt(item[25]),Integer.parseInt(item[26]),Integer.parseInt(item[27]),Integer.parseInt(item[28]),Float.parseFloat(item[29]),Float.parseFloat(item[30]),Float.parseFloat(item[31]),Float.parseFloat(item[32]),Float.parseFloat(item[33]),
	            		  Float.parseFloat(item[34]),Float.parseFloat(item[35]),Float.parseFloat(item[36]),Float.parseFloat(item[37]),Float.parseFloat(item[38]),Integer.parseInt(item[39]),Integer.parseInt(item[40]),Integer.parseInt(item[41]),Integer.parseInt(item[42]),Integer.parseInt(item[43]),Integer.parseInt(item[44]),
	            				  Integer.parseInt(item[45]),Integer.parseInt(item[46]),Integer.parseInt(item[47]),Integer.parseInt(item[48]),Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),
	            				  Float.parseFloat(item[56]),Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63])));   
	          }/**/							
			reader.close();	
			read_done = true;
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	private Queue <data> queue;
}

class processTask implements Runnable{
	public processTask (Queue <data> queue, Map<String, LinkedList<data>> map) {
		this.rawData = queue;		
		this.outData = map;
	}
	public void run() {
		try {
			System.out.println("Process task is running");
			int i = 0;
			while(true) {
				if(readTask.read_done==true) {
					if(rawData.isEmpty()) {
						break;
					}					
				}
				
				if(rawData.isEmpty()) {
					continue;					
				}
				else {			
					BADeal();
					LPDeal();
					TODeal();
					toOutData();
					rawData.poll();			
					System.out.println("Process data:"+i);
					i++;						
				}
								
			}			
		}catch (Exception e) {}		
	}
	private void BADeal() throws Exception{
		data d = rawData.element();
		if(d.HighLimited!=0 || d.LowLimited!=0) {
			return;
		}
		else {
			for(int i=0;i<d.AskPrice.length;i++) {
				if(d.AskPrice[i]!=0) {
					if(d.AskPrice[i]>d.HighLimited||d.AskPrice[i]<d.LowLimited) {
						outLog(d,"AskPrice wrong:");
					}					
				}
				if(d.BidPrice[i]!=0) {
					if(d.BidPrice[i]>d.HighLimited||d.BidPrice[i]<d.LowLimited) {
						outLog(d,"BidPrice wrong:");
					}
				}
			}
		}
		
	}
	private void LPDeal() throws Exception{
		data d = rawData.element();		
		if(d.newPrice==0&&d.Volume==0) {
			return;
		}
		else if(d.newPrice==0&&d.Volume!=0) {
			outLog(d,"Price is 0,but volume isn't 0:");
		}
		else {
			if(d.newPrice>d.HighLimited) {
				outLog(d,"Price more than HighLimited:");
			}
			if(d.newPrice<d.LowLimited) {
				outLog(d,"Prie less than LowLimited:");
			}
		}
	}
	private void TODeal() throws Exception{
		data d = rawData.element();
		String str = "TurnOver wrong:";
		if(d.Turnover<d.afterTurnover) {
			outLog(d,str);
		}
	}
	private void outLog(data d,String s) throws Exception{
		File file = new File("ProcessLog.log");
		if(!file.exists()) {
			file.createNewFile();
		}		
		FileWriter fw = new FileWriter(file,true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(s+" Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
		bw.close();
	}
	private void toOutData() {
		data d = rawData.element();
		String key = d.code + d.market;
		LinkedList<data> lst;
		if (!outData.containsKey(key)) {
			lst = new LinkedList<data>();
			lst.add(d);
			outData.put(key, lst);		
		}
		else {
			lst = outData.get(key);
			lst.add(d);
			outData.replace(key, lst);
		}
	}
	private Queue <data> rawData;	
	private Map<String, LinkedList<data>> outData;
}

class outTask  {
	public outTask(Map <String,LinkedList<data>> map) {
		this.finalData = map;
	}
	
	public void run_outTask() {
		String FILENAME = "F:\\Java\\Test\\src\\ReadData\\data.h5";    	
        long file_id = -1;
        long group_id = -1;        
        try {
            file_id = H5.H5Fcreate(FILENAME, HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT,
                    HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }        
		for (Map.Entry<String, LinkedList<data>> entry : finalData.entrySet()) {
			LinkedList<data> lst = entry.getValue();
			int size = lst.size();
			long[] dims = { size };	
			long[] dims_2 = {size,10};
						
			if (file_id >= 0) {
	            group_id = H5.H5Gcreate(file_id, "/" + entry.getKey(), HDF5Constants.H5P_DEFAULT,
	                    HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
	        }			
		    write_uInt32(group_id, "time", get_time(lst,size), dims);
		    write_uInt32(group_id, "status", get_status(lst,size), dims);
		    write_Folat32(group_id, "PerClose", get_PerClose(lst,size),dims);
		    write_Folat32(group_id, "open", get_open(lst,size),dims);
		    write_Folat32(group_id, "high", get_high(lst,size),dims);
		    write_Folat32(group_id, "low", get_low(lst,size),dims);
		    write_Folat32(group_id, "NewPrice", get_NewPrice(lst,size),dims);
		    write_Int64(group_id, "volume", get_volume(lst,size),dims);
		    write_Folat64(group_id, "turnover", get_turnover(lst,size),dims);		    
		    write_Float32_in2dim(group_id, "AskPrice", get_AskPrice(lst,size),dims_2);
		    write_Float32_in2dim(group_id, "BidPrice", get_BidPrice(lst,size),dims_2);
		    write_uInt32_in2dim(group_id, "AskVolume", get_AskVolume(lst,size),dims_2);
		    write_uInt32_in2dim(group_id, "BidVolume", get_BidVolume(lst,size),dims_2);
		    write_Folat32(group_id, "AskAvgPrice", get_AskAvgPrice(lst,size),dims);
		    write_Folat32(group_id, "BidAvgPrice", get_BidAvgPrice(lst,size),dims);
		    write_Int64(group_id, "TotalAskVolume", get_TotalAskVolume(lst,size),dims);
		    write_Int64(group_id, "TotalBidVolume", get_TotalBidVolume(lst,size),dims);
		    write_Folat32(group_id, "HighLimited", get_HighLimited(lst,size),dims);
		    write_Folat32(group_id, "LowLimited", get_LowLimited(lst,size),dims);
		    write_uInt64(group_id, "LocalTime", get_LocalTime(lst,size),dims);
		    try {
		    	if(group_id>=0) {
		    		H5.H5Gclose(group_id);		    
		    	}
		    	group_id = -1;
		    }catch (Exception e) {
	            e.printStackTrace();
	        }
		}	
		try {
	    	if(file_id>=0) {
	    		H5.H5Fclose(file_id);		    
	    	}	    	
	    }catch (Exception e) {
            e.printStackTrace();
        }

	}
	
	

	private void write_uInt32(long group_id, String dataset_name, int []data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(1, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, dataset_name, HDF5Constants.H5T_STD_I32BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_UINT32, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	private void write_uInt64(long group_id, String data_name, long []data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(1, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_STD_I64BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_UINT64, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	private void write_Folat32(long group_id, String data_name, float []data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(1, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_IEEE_F32BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	private void write_Folat64(long group_id, String data_name, double []data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(1, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_IEEE_F64BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	private void write_Int64(long group_id, String data_name, long []data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(1, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_STD_I64BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT64, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	private void write_Float32_in2dim(long group_id, String data_name, float [][]data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(2, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_IEEE_F32BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	private void write_uInt32_in2dim(long group_id, String data_name, int [][]data, long[] dims) {
		long dataspace_id = -1;
		long dataset_id = -1;
		try {
            dataspace_id = H5.H5Screate_simple(2, dims, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if ((group_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(group_id, data_name, HDF5Constants.H5T_STD_I32BE,
                        dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0) 
            	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_UINT32, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, data);	            	            	                
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
            dataspace_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
            dataset_id = -1;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	private int[] get_time(LinkedList<data> lst, int size) {
		int time[] = new int[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			time[index] = iter.next().Time;
			index++;
		}
		return time;
	}
	private int[] get_status(LinkedList<data> lst, int size) {
		int status[] = new int[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			status[index] = iter.next().status;
			index++;
		}
		return status;
	}
	private float[] get_PerClose(LinkedList<data> lst, int size) {
		float perclose[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			perclose[index] = iter.next().PreClose;
			index++;
		}
		return perclose;
	}
	private float[] get_open(LinkedList<data> lst, int size) {
		float open[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			open[index] = iter.next().Open;
			index++;
		}
		return open;
	}
	private float[] get_high(LinkedList<data> lst, int size) {
		float high[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			high[index] = iter.next().High;
			index++;
		}
		return high;
	}
	private float[] get_low(LinkedList<data> lst, int size) {
		float low[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			low[index] = iter.next().Low;
			index++;
		}
		return low;
	}
	private float[] get_NewPrice(LinkedList<data> lst, int size) {
		float newprice[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			newprice[index] = iter.next().newPrice;
			index++;
		}
		return newprice;
	}
	
	private long[] get_volume(LinkedList<data> lst, int size) {
		long volume[] = new long[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			volume[index] = iter.next().Volume;
			index++;
		}
		return volume;
	}
	private double[] get_turnover(LinkedList<data> lst, int size) {
		double turnover[] = new double[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			turnover[index] = iter.next().Turnover;
			index++;
		}
		return turnover;
	}
	private long[] get_TotalBidVolume(LinkedList<data> lst, int size) {
		long TotalBidVolume[] = new long[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			TotalBidVolume[index] = iter.next().TotalBidVolume;
			index++;
		}
		return TotalBidVolume;
	}
	private long[] get_TotalAskVolume(LinkedList<data> lst, int size) {
		long TotalAskVolume[] = new long[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			TotalAskVolume[index] = iter.next().TotalAskVolume;
			index++;
		}
		return TotalAskVolume;
	}
	private long[] get_LocalTime(LinkedList<data> lst, int size) {
		long LocalTime[] = new long[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			LocalTime[index] = iter.next().LocalTime;
			index++;
		}
		return LocalTime;
	}
	
	private float[] get_HighLimited(LinkedList<data> lst, int size) {
		float HighLimited[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			HighLimited[index] = iter.next().HighLimited;
			index++;
		}
		return HighLimited;
	}
	private float[] get_LowLimited(LinkedList<data> lst, int size) {
		float LowLimited[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			LowLimited[index] = iter.next().LowLimited;
			index++;
		}
		return LowLimited;
	}	
	
	private float[][]get_AskPrice(LinkedList<data> lst, int size){
		float askPrice[][] = new float[size][10];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			data d = iter.next();
			for(int i=0;i<10;i++) {
				askPrice[index][i] = d.AskPrice[i];
			}
			index++;
		}
		return askPrice;
	}
	private float[][]get_BidPrice(LinkedList<data> lst, int size){
		float bidPrice[][] = new float[size][10];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			data d = iter.next();
			for(int i=0;i<10;i++) {
				bidPrice[index][i] = d.BidPrice[i];
			}
			index++;
		}
		return bidPrice;
	}
	private int[][]get_AskVolume(LinkedList<data> lst, int size){
		int bidVolume[][] = new int[size][10];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			data d = iter.next();
			for(int i=0;i<10;i++) {
				bidVolume[index][i] = d.AskVolume[i];
			}
			index++;
		}
		return bidVolume;
	}
	private int[][]get_BidVolume(LinkedList<data> lst, int size){
		int bidVolume[][] = new int[size][10];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			data d = iter.next();
			for(int i=0;i<10;i++) {
				bidVolume[index][i] = d.BidVolume[i];
			}
			index++;
		}
		return bidVolume;
	}	
	private float[]get_AskAvgPrice(LinkedList<data> lst, int size){
		float askAvgPrice[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {			
			askAvgPrice[index] = iter.next().weightedAvgAskPx;			
			index++;
		}
		return askAvgPrice;
	}
	private float[]get_BidAvgPrice(LinkedList<data> lst, int size){
		float bidAvgPrice[] = new float[size];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {			
			bidAvgPrice[index] = iter.next().weightedAvgBidPx;			
			index++;
		}
		return bidAvgPrice;
	}
	
	private Map <String, LinkedList<data>> finalData;
}


class data{ 
	String market;
	int code;
	int Time;
	int status;
	float PreClose;
    float Open;
    float High;
    float Low;
    float newPrice;
    float AskPrice[]=new float[10];
    int AskVolume[]=new int[10];
    float BidPrice[]=new float[10];
    int BidVolume[]=new int[10];
    float matchItems;
    long Volume;
    double Turnover;
    long TotalBidVolume;
    long TotalAskVolume;
    float IOPV;
    float HighLimited;
    float LowLimited;
    float afterPrice;
    float afterVolume;
    float afterTurnover;
    float afterMatchItems;
    long LocalTime;
    float weightedAvgAskPx;
    float weightedAvgBidPx;    
    data(String market,int code,int Time, int status, float PreClose,float Open,float High,float Low,float newPrice,
    		float AskPrice0,float AskPrice1,float AskPrice2,float AskPrice3,float AskPrice4,float AskPrice5,float AskPrice6,float AskPrice7,float AskPrice8,float AskPrice9,int AskVolume0,int AskVolume1,int AskVolume2,int AskVolume3,int AskVolume4,int AskVolume5,int AskVolume6,int AskVolume7,int AskVolume8,int AskVolume9,float BidPrice0,float BidPrice1,float BidPrice2,float BidPrice3,float BidPrice4,float BidPrice5,float BidPrice6,float BidPrice7,float BidPrice8,float BidPrice9,int BidVolume0,int BidVolume1,int BidVolume2,int BidVolume3,int BidVolume4,int BidVolume5,int BidVolume6,int BidVolume7,int BidVolume8,int BidVolume9,float matchItems,long Volume,double Turnover,long TotalBidVolume,long TotalAskVolume,float IOPV,float HighLimited,float LowLimited,float afterPrice,float afterVolume,float afterTurnover,float afterMatchItems,long LocalTime,float weightedAvgAskPx,float weightedAvgBidPx){  
    	this.market = market;
    	this.code = code;   
    	this.Time= Time;
    	this.status= status;
    	this.PreClose= PreClose;
    	this.Open= Open;
    	this.High= High;
    	this.Low= Low;
    	this.newPrice= newPrice;
    	this.AskPrice[0]= AskPrice0;
    	this.AskPrice[1]= AskPrice1;
    	this.AskPrice[2]= AskPrice2;
    	this.AskPrice[3]= AskPrice3;
    	this.AskPrice[4]= AskPrice4;
    	this.AskPrice[5]= AskPrice5;
    	this.AskPrice[6]= AskPrice6;
    	this.AskPrice[7]= AskPrice7;
    	this.AskPrice[8]= AskPrice8;
    	this.AskPrice[9]= AskPrice9;
    	this.AskVolume[0]= AskVolume0;
    	this.AskVolume[1]= AskVolume1;
    	this.AskVolume[2]= AskVolume2;
    	this.AskVolume[3]= AskVolume3;
    	this.AskVolume[4]= AskVolume4;
    	this.AskVolume[5]= AskVolume5;
    	this.AskVolume[6]= AskVolume6;
    	this.AskVolume[7]= AskVolume7;
    	this.AskVolume[8]= AskVolume8;
    	this.AskVolume[9]= AskVolume9;
    	this.BidPrice[0]= BidPrice0;
    	this.BidPrice[1]= BidPrice1;
    	this.BidPrice[2]= BidPrice2;
    	this.BidPrice[3]= BidPrice3;
    	this.BidPrice[4]= BidPrice4;
    	this.BidPrice[5]= BidPrice5;
    	this.BidPrice[6]= BidPrice6;
    	this.BidPrice[7]= BidPrice7;
    	this.BidPrice[8]= BidPrice8;
    	this.BidPrice[9]= BidPrice9;
    	this.BidVolume[0]= BidVolume0;
    	this.BidVolume[1]= BidVolume1;
    	this.BidVolume[2]= BidVolume2;
    	this.BidVolume[3]= BidVolume3;
    	this.BidVolume[4]= BidVolume4;
    	this.BidVolume[5]= BidVolume5;
    	this.BidVolume[6]= BidVolume6;
    	this.BidVolume[7]= BidVolume7;
    	this.BidVolume[8]= BidVolume8;
    	this.BidVolume[9]= BidVolume9;
    	this.matchItems= matchItems;
    	this.Volume= Volume;
    	this.Turnover= Turnover;
    	this.TotalBidVolume= TotalBidVolume;
    	this.TotalAskVolume= TotalAskVolume;
    	this.IOPV= IOPV;
    	this.HighLimited= HighLimited;
    	this.LowLimited= LowLimited;
    	this.afterPrice= afterPrice;
    	this.afterVolume= afterVolume;
    	this.afterTurnover= afterTurnover;
    	this.afterMatchItems= afterMatchItems;
    	this.LocalTime= LocalTime;
    	this.weightedAvgAskPx= weightedAvgAskPx;
    	this.weightedAvgBidPx= weightedAvgBidPx;
   
    }   
}




/*
// time Int32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "time", HDF5Constants.H5T_STD_I32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_time(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}


// status Int32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "status", HDF5Constants.H5T_STD_I32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_status(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}


// PerClose Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "PreClose", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_PerClose(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// Open Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "open", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_open(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// high Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "high", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_high(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// low Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "low", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_low(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// newPrice Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "NewPrice", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_NewPrice(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

/*
// matchItems Float32
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "MatchItems", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, matchItems);	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}


// volume Int64
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "volume", HDF5Constants.H5T_STD_I64BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_volume(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// turnover Float64
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "TurnOver", HDF5Constants.H5T_IEEE_F64BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_turnover(lst, size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}	               

// AskPrice Float32	     
try {
    dataspace_id = H5.H5Screate_simple(2, dims_2, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "AskPrice", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_AskPrice(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// BidPrice Float32	     
try {
    dataspace_id = H5.H5Screate_simple(2, dims_2, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "BidPrice", HDF5Constants.H5T_IEEE_F32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_BidPrice(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// AskVolume Int32	     
try {
    dataspace_id = H5.H5Screate_simple(2, dims_2, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "AskVolume", HDF5Constants.H5T_STD_I32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_AskVolume(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// BidVolume Int32	     
try {
    dataspace_id = H5.H5Screate_simple(2, dims_2, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "BidVolume", HDF5Constants.H5T_STD_I32BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_BidVolume(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// TotalBidVolume Int64
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "TotalBidVolume", HDF5Constants.H5T_STD_I64BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_TotalBidVolume(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}

// TotalAskVolume Int64
try {
    dataspace_id = H5.H5Screate_simple(1, dims, null);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if ((group_id >= 0) && (dataspace_id >= 0))
        dataset_id = H5.H5Dcreate(group_id, "TotalAskVolume", HDF5Constants.H5T_STD_I64BE,
                dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0) 
    	H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, get_TotalAskVolume(lst,size));	            	            	                
}catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataspace_id >= 0)
        H5.H5Sclose(dataspace_id);
    dataspace_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
try {
    if (dataset_id >= 0)
        H5.H5Dclose(dataset_id);
    dataset_id = -1;
}
catch (Exception e) {
    e.printStackTrace();
}
*/
















/*
 * private float[] get_MatchItems(LinkedList<data> lst, int size) {
	float matchitems[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		matchitems[index] = iter.next().Time;
	}
	return matchitems;
}
private float[] get_IOPV(LinkedList<data> lst, int size) {
	float IOPV[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		IOPV[index] = iter.next().Time;
	}
	return IOPV;
}
private float[] get_AfterPrice(LinkedList<data> lst, int size) {
	float AfterPrice[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		AfterPrice[index] = iter.next().Time;
	}
	return AfterPrice;
}
private float[] get_AfterVolume(LinkedList<data> lst, int size) {
	float AfterVolume[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		AfterVolume[index] = iter.next().Time;
	}
	return AfterVolume;
}
private float[] get_AfterTurnover(LinkedList<data> lst, int size) {
	float AfterTurnover[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		AfterTurnover[index] = iter.next().Time;
	}
	return AfterTurnover;
}
private float[] get_AfterMatchItems(LinkedList<data> lst, int size) {
	float AfterMatchItems[] = new float[size];
	int index = 0;
	Iterator<data> iter = lst.iterator();
	while(iter.hasNext()) {
		AfterMatchItems[index] = iter.next().Time;
	}
	return AfterMatchItems;
}*/