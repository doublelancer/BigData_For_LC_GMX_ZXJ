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
import java.util.Map.Entry;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;


public class DataProcess {	
	public static void main(String [] args) {
		DataProcess task1 = new DataProcess();
		long startTime = System.currentTimeMillis();
		outTask task3 = new outTask(map);		
		file = new File("ProcessLog.log");
		try {
			if(!file.exists()) {
				file.createNewFile();
			}		
			fw = new FileWriter(file);
			bw = new BufferedWriter(fw);		
			BufferedWriter write_data_num = new BufferedWriter(new FileWriter(new File("count.log")));
			
			task1.read_data();
			task1.traverse_process();
		
			write_data_num.write("Total data:"+data_num+"\n");
			write_data_num.write("Total price error:"+price_error+"\n");
			write_data_num.write("Total new price error:"+newprice_error+"\n");
			write_data_num.write("Total ask1&bid1 error:"+ask1_bid1_error+"\n");		
			write_data_num.write("Total limited error:"+limit_error+"\n");
			write_data_num.write("Total more than 30s error:"+long_interval_error+"\n");
			write_data_num.write("Total time error:"+time_error+"\n");
			write_data_num.write("Total volume error:"+volume_error+"\n");
			write_data_num.write("Total turnover error:"+turnover_error+"\n");
			write_data_num.write("Total high error:"+high_error+"\n");
			write_data_num.write("Total low error:"+low_error+"\n");
			write_data_num.close();
			bw.close();	
		}catch (Exception e) {
            e.printStackTrace();
        }			
		try {		
			task3.run_outTask();
		}catch (Exception e) {
			e.printStackTrace();
		}
			
		long endTime = System.currentTimeMillis();
		System.out.println("³ÌÐòÔËÐÐÊ±¼ä£º" + (endTime - startTime) + "ms");
	}
	
	private void read_data() {		
		try {
			String filename = "F:\\Java\\Test\\src\\ReadData\\data.txt";
			File file = new File(filename);
			BufferedReader reader = null;		
			String strTemp;
			String []item;
			data d;
			reader = new BufferedReader(new FileReader(file));
			strTemp = reader.readLine();
			
			data_num++;
			item = strTemp.split(",");	
			d = new data(item[0],Integer.parseInt(item[1]),Integer.parseInt(item[2]),Integer.parseInt(item[3]),Float.parseFloat(item[4]),Float.parseFloat(item[5]),Float.parseFloat(item[6]),Float.parseFloat(item[7]),Float.parseFloat(item[8]),Float.parseFloat(item[9]),Float.parseFloat(item[10]),Float.parseFloat(item[11]),
            		  Float.parseFloat(item[12]),Float.parseFloat(item[13]),Float.parseFloat(item[14]),Float.parseFloat(item[15]),Float.parseFloat(item[16]),Float.parseFloat(item[17]),Float.parseFloat(item[18]),Integer.parseInt(item[19]),Integer.parseInt(item[20]),Integer.parseInt(item[21]),Integer.parseInt(item[22]),
            		  Integer.parseInt(item[23]),Integer.parseInt(item[24]),Integer.parseInt(item[25]),Integer.parseInt(item[26]),Integer.parseInt(item[27]),Integer.parseInt(item[28]),Float.parseFloat(item[29]),Float.parseFloat(item[30]),Float.parseFloat(item[31]),Float.parseFloat(item[32]),Float.parseFloat(item[33]),
            		  Float.parseFloat(item[34]),Float.parseFloat(item[35]),Float.parseFloat(item[36]),Float.parseFloat(item[37]),Float.parseFloat(item[38]),Integer.parseInt(item[39]),Integer.parseInt(item[40]),Integer.parseInt(item[41]),Integer.parseInt(item[42]),Integer.parseInt(item[43]),Integer.parseInt(item[44]),
            				  Integer.parseInt(item[45]),Integer.parseInt(item[46]),Integer.parseInt(item[47]),Integer.parseInt(item[48]),Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),
            				  Float.parseFloat(item[56]),Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63]));
			if(d.market == "SZ") {
				last_data_time_marketsz = d.LocalTime;
			}				
			else if(d.market == "SH") {
				last_data_time_marketsh = d.LocalTime;				
			}
			open_time = d.LocalTime;
			put_to_map(d);
			
			while((strTemp = reader.readLine()) != null) {
				data_num++;
				item = strTemp.split(",");	
				d = new data(item[0],Integer.parseInt(item[1]),Integer.parseInt(item[2]),Integer.parseInt(item[3]),Float.parseFloat(item[4]),Float.parseFloat(item[5]),Float.parseFloat(item[6]),Float.parseFloat(item[7]),Float.parseFloat(item[8]),Float.parseFloat(item[9]),Float.parseFloat(item[10]),Float.parseFloat(item[11]),
	            		  Float.parseFloat(item[12]),Float.parseFloat(item[13]),Float.parseFloat(item[14]),Float.parseFloat(item[15]),Float.parseFloat(item[16]),Float.parseFloat(item[17]),Float.parseFloat(item[18]),Integer.parseInt(item[19]),Integer.parseInt(item[20]),Integer.parseInt(item[21]),Integer.parseInt(item[22]),
	            		  Integer.parseInt(item[23]),Integer.parseInt(item[24]),Integer.parseInt(item[25]),Integer.parseInt(item[26]),Integer.parseInt(item[27]),Integer.parseInt(item[28]),Float.parseFloat(item[29]),Float.parseFloat(item[30]),Float.parseFloat(item[31]),Float.parseFloat(item[32]),Float.parseFloat(item[33]),
	            		  Float.parseFloat(item[34]),Float.parseFloat(item[35]),Float.parseFloat(item[36]),Float.parseFloat(item[37]),Float.parseFloat(item[38]),Integer.parseInt(item[39]),Integer.parseInt(item[40]),Integer.parseInt(item[41]),Integer.parseInt(item[42]),Integer.parseInt(item[43]),Integer.parseInt(item[44]),
	            				  Integer.parseInt(item[45]),Integer.parseInt(item[46]),Integer.parseInt(item[47]),Integer.parseInt(item[48]),Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),
	            				  Float.parseFloat(item[56]),Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63]));
				if(d.market == "SZ") {
					if(last_data_time_marketsz == 0) {
						last_data_time_marketsz = d.LocalTime;
					}
					else {
						inter_more_than_30s(d,last_data_time_marketsz);
						last_data_time_marketsz = d.LocalTime;
					}
				}
				else if(d.market == "SH") {
					if(last_data_time_marketsh == 0) {
						last_data_time_marketsh = d.LocalTime;
					}
					else {
						inter_more_than_30s(d,last_data_time_marketsh);
						last_data_time_marketsh = d.LocalTime;
					}
				}
				put_to_map(d);
	          }						
			reader.close();				
		}catch(Exception e) {
			e.printStackTrace();
		}		
	}	
	private void put_to_map(data d) {		
		String key = d.code + d.market;		
		LinkedList<data> lst;
		if (!map.containsKey(key)) {
			lst = new LinkedList<data>();
			lst.add(d);
			map.put(key, lst);		
		}
		else {
			lst = map.get(key);
			lst.add(d);
			map.replace(key, lst);
		}
	}
	private void inter_more_than_30s(data d, long last_time) throws Exception{
		if((d.LocalTime-last_time)>30000) {
			long_interval_error++;
			bw.write("Lost data, above data: "+d.market + d.code + d.LocalTime+"\n");
		}
	}
	
	private void price_deal(data d) throws Exception{
		boolean AskPriceFalse = false;
		if(d.HighLimited!=0 || d.LowLimited!=0) {
			return;
		}
		else {
			for(int i=0;i<d.AskPrice.length;i++) {
				if(d.AskPrice[i]!=0) {
					if(d.AskPrice[i]>d.HighLimited||d.AskPrice[i]<d.LowLimited) {
						AskPriceFalse = true;
						price_error++;
						break;						
					}		
					else return;
				}
				if(d.BidPrice[i]!=0) {
					if(d.BidPrice[i]>d.HighLimited||d.BidPrice[i]<d.LowLimited) {
						price_error++;
						break;					
					}
					else return;
				}
			}
			if (AskPriceFalse) bw.write("AskPrice error: Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
			else bw.write("AskPrice error: Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
		}		
	}
	
	private void ask1_bid1_deal(data d) throws Exception{		 
		if(d.AskPrice[0]==0&&d.BidPrice[0]==0) {				
			String temps=d.LocalTime+"";
			temps.substring(8);
			long templ=Long.parseLong(temps);
			if(!((templ>=145600000&&templ<=145800000)||(d.LocalTime-open_time<=100000))) {
				ask1_bid1_error++;
				bw.write("AskPrice1 $ BidPrice1 error: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");				
			}					 
		}			
	}
	private void last_price_deal(data d) throws Exception{				
		if(d.newPrice==0&&d.Volume==0) {
			return;
		}
		else if(d.newPrice==0&&d.Volume!=0) {
			newprice_error++;
			bw.write("Price is 0,but volume isn't 0: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");			
		}
		else {
			if(d.newPrice>d.HighLimited) {
				newprice_error++;
				bw.write("Price more than HighLimited: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
			}
			if(d.newPrice<d.LowLimited) {
				newprice_error++;
				bw.write("Price less than LowLimited: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
			}
		}
	}	
	
	private void limited_deal(data d) throws Exception{
		String str = "Limited wrong:";
		if (d.HighLimited<=0||d.LowLimited<=0) {
			limit_error++;
			bw.write("Limited error: "+d.market+d.code+d.LocalTime+"\n");			
		}
	}
	
	private void traverse_process() {
		int last_time;
		double last_turnover;
		float last_high;
		float last_low;
		long last_volume;
		data next_d;
		data d;
		Iterator<Map.Entry<String, LinkedList<data>>> iterator_map = map.entrySet().iterator();
		while (iterator_map.hasNext()) {
		    Map.Entry<String, LinkedList<data>> entry = iterator_map.next();	
		    String key = entry.getKey();
		    LinkedList<data> lst = entry.getValue();
		    Iterator<data> iterator_list = lst.iterator();
		    d = iterator_list.next(); 		    
		    while(iterator_list.hasNext()) {
		    	next_d = iterator_list.next();	
		    	try {
				    limited_deal(d);
				    ask1_bid1_deal(d);
				    price_deal(d);
				    last_price_deal(d);	
			    
			    	if(d.Time>next_d.Time) {
				    	time_error++;
				    	bw.write("Time increase error: " + d.market + d.code + d.LocalTime+"\n");
				    }
				    if(d.Volume>next_d.Volume) {
				    	volume_error++;
				    	bw.write("Volume increase error: " + d.market + d.code + d.LocalTime+"\n");
				    }
				    if(d.Turnover>next_d.Turnover) {
				    	turnover_error++;
				    	lst.remove(d);
				    	bw.write("Turnover increase error: " + d.market + d.code + d.LocalTime+"\n");
				    }
				    if(d.High>next_d.High) {
					    high_error++;
					    bw.write("High increase error: " + d.market + d.code + d.LocalTime+"\n");
					}
				    if(d.Low<next_d.Low) {
					    low_error++;
					    bw.write("Low increase error: " + d.market + d.code + d.LocalTime+"\n");
					}
			    } catch (Exception e) {
		            e.printStackTrace();
		        }			    			    
			    d = next_d;
		    }
		    try {
		    	limited_deal(d);
			    ask1_bid1_deal(d);
			    price_deal(d);
			    last_price_deal(d);
		    }catch (Exception e) {
	            e.printStackTrace();
	        }		    
		    map.replace(key, lst);
		}
	}
	
	public static Map<String,LinkedList<data>> map = new HashMap<String, LinkedList<data>>();
	private static long open_time;
	private static int limit_error = 0;
	private static int ask1_bid1_error = 0;
	private static int price_error = 0;
	private static int newprice_error = 0;	
	private static int long_interval_error = 0;
	private static int time_error = 0;
	private static int turnover_error = 0;
	private static int high_error = 0;
	private static int low_error = 0;
	private static int volume_error = 0;	
	private static int data_num = 0;
	public static File file;
	public static FileWriter fw;
	public static BufferedWriter bw;
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
		int askVolume[][] = new int[size][10];
		int index = 0;
		Iterator<data> iter = lst.iterator();
		while(iter.hasNext()) {
			data d = iter.next();
			for(int i=0;i<10;i++) {
				askVolume[index][i] = d.AskVolume[i];
			}
			index++;
		}
		return askVolume;
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
class codeCompare implements Comparator<data>{
	@Override
	public int compare(data d1, data d2) {
		if(d1.Time>=d2.Time) {
			return 1;
		}
		else 
			return 0;
	}
}
*/
