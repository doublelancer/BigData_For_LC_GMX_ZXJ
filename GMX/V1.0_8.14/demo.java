import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class a {
	public static void main(String [] args) {
		try {
			Queue <data> data0 = new ConcurrentLinkedQueue<data>();			
			
			readTask task1 = new readTask(data0);
			processTask task2 = new processTask(data0);			
			
			Thread thread1 = new Thread(task1);
			Thread thread2 = new Thread(task2);			
			
			thread1.start();
			thread2.start();			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}	
}

class readTask implements Runnable{
	
	public readTask(Queue <data> queue ) {
		this.queue = queue;		
	}
	public void run() {
		try {
			String filename = "data.txt";
			File file = new File(filename);
			BufferedReader reader = null;		
			String strTemp;
			reader = new BufferedReader(new FileReader(file));
			while((strTemp = reader.readLine()) != null) {
				String []item = strTemp.split(",");	
				queue.add(new data(item[0],Integer.parseInt(item[1]),Integer.parseInt(item[2]),item[3],Float.parseFloat(item[4]),Float.parseFloat(item[5]),Float.parseFloat(item[6]),Float.parseFloat(item[7]),Float.parseFloat(item[8]),Float.parseFloat(item[9]),Float.parseFloat(item[10]),Float.parseFloat(item[11]),
	            		  Float.parseFloat(item[12]),Float.parseFloat(item[13]),Float.parseFloat(item[14]),Float.parseFloat(item[15]),Float.parseFloat(item[16]),Float.parseFloat(item[17]),Float.parseFloat(item[18]),Integer.parseInt(item[19]),Integer.parseInt(item[20]),Integer.parseInt(item[21]),Integer.parseInt(item[22]),
	            		  Integer.parseInt(item[23]),Integer.parseInt(item[24]),Integer.parseInt(item[25]),Integer.parseInt(item[26]),Integer.parseInt(item[27]),Integer.parseInt(item[28]),Float.parseFloat(item[29]),Float.parseFloat(item[30]),Float.parseFloat(item[31]),Float.parseFloat(item[32]),Float.parseFloat(item[33]),
	            		  Float.parseFloat(item[34]),Float.parseFloat(item[35]),Float.parseFloat(item[36]),Float.parseFloat(item[37]),Float.parseFloat(item[38]),Integer.parseInt(item[39]),Integer.parseInt(item[40]),Integer.parseInt(item[41]),Integer.parseInt(item[42]),Integer.parseInt(item[43]),Integer.parseInt(item[44]),
	            				  Integer.parseInt(item[45]),Integer.parseInt(item[46]),Integer.parseInt(item[47]),Integer.parseInt(item[48]),Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),
	            				  Float.parseFloat(item[56]),Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63])));   
	          }/**/							
			reader.close();			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	private Queue <data> queue;
}

class processTask implements Runnable{
	public processTask (Queue <data> queue) {
		this.rawData = queue;		
	}
	public void run() {
		try {
			System.out.println("Process task is running");
			int i = 0;
			while(true) {
				if(rawData.isEmpty()) {
					continue;					
				}
				else {			
					BADeal();
					LPDeal();
					TODeal();
					rawData.poll();			
					System.out.println("Process data:"+i);
					i++;
					if(i==36) {
						break;
					}
				}
			}			
		}catch (Exception e) {}		
	}
	public void BADeal() throws Exception{
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
	public void LPDeal() throws Exception{
		data d = rawData.element();
		String str = "Price wrong:";
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
	public void TODeal() throws Exception{
		data d = rawData.element();
		String str = "TurnOver wrong:";
		if(d.Turnover<d.afterTurnover) {
			outLog(d,str);
		}
	}
	public void outLog(data d,String s) throws Exception{
		File file = new File("ProcessLog.log");
		if(!file.exists()) {
			file.createNewFile();
		}		
		FileWriter fw = new FileWriter(file,true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(s+" Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
		bw.close();
	}
	private Queue <data> rawData;	
}

// 写数据到文件部分，目前没实现
/*class outTask implements Runnable {
	public outTask(ConcurrentHashMap <Integer,List<String[]>> hashmap) {
		this.finalData = hashmap;
	}
	
	public void run() {
		try {
			FileOutputStream os = new FileOutputStream("F:\\Java\\Test\\src\\ReadFile\\processedData");
			OutputStreamWriter out = new OutputStreamWriter(os);
			List<String> list = new ArrayList<>();
			while(true) {
				if(finalData.isEmpty()) {
					continue;
				}
				else {
					
				}
			}	
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	void AskAndBid () {
		
	}
	private ConcurrentHashMap <Integer,List<String[]>> finalData;
}*/

class data{ 
	String market;  //0
	int code;       //1
	int Time;        //2
	String status;  //3
	float PreClose;//4
    float Open;    //5
    float High;   //6
    float Low;//7
    float newPrice;//8        ??????
    float AskPrice[]=new float[10];//9-18
    int AskVolume[]=new int[10];//19-28
    float BidPrice[]=new float[10];//29-38
    int BidVolume[]=new int[10];//39-48
    float matchItems; // 49    ??????????????? 
    long Volume;  //50
    double Turnover; //51
    long TotalBidVolume;//52
    long TotalAskVolume;//53
    float IOPV;//54
    float HighLimited;   //55
    float LowLimited; //56
    float afterPrice;//57
    float afterVolume;//58
    float afterTurnover;//59
    float afterMatchItems;//60
    long LocalTime;//61
    float weightedAvgAskPx;//62
    float weightedAvgBidPx;//63
    
    data(String market,int code,int Time, String status, float PreClose,float Open,float High,float Low,float newPrice,
    		float AskPrice0,float AskPrice1,float AskPrice2,float AskPrice3,float AskPrice4,float AskPrice5,float AskPrice6,float AskPrice7,float AskPrice8,float AskPrice9,
    		int AskVolume0,int AskVolume1,int AskVolume2,int AskVolume3,int AskVolume4,int AskVolume5,int AskVolume6,int AskVolume7,int AskVolume8,int AskVolume9,
    		float BidPrice0,float BidPrice1,float BidPrice2,float BidPrice3,float BidPrice4,float BidPrice5,float BidPrice6,float BidPrice7,float BidPrice8,float BidPrice9,
    		int BidVolume0,int BidVolume1,int BidVolume2,int BidVolume3,int BidVolume4,int BidVolume5,int BidVolume6,int BidVolume7,int BidVolume8,int BidVolume9,
    		float matchItems,long Volume,double Turnover,long TotalBidVolume,long TotalAskVolume,float IOPV,float HighLimited,float LowLimited,float afterPrice,float afterVolume,float afterTurnover,
    	    float afterMatchItems,
    	    long LocalTime,
    	    float weightedAvgAskPx,
    	    float weightedAvgBidPx
    		){  
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








