package aef;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;



public class DataClean {
	Vector<data> group=new Vector<data>();  //储存原始数据
	Vector<data> codegroup=new Vector<data>();  //存储
	
	  public static void main(String[] args) { 
          try { 
        	  DataClean dc1=new DataClean();
        	  dc1.readData();
        	  dc1.timeDeal();
        	  dc1.volumeDeal();
        	  dc1.LimitedDeal();
        	  dc1.writeData();
              System.out.println("完成");
          } catch (Exception e) { 
              e.printStackTrace(); 
          } 
      }
	  
	  public void readData() throws IOException{
		   BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\double lancer\\Desktop\\exame.csv"), "utf-8"));//GBK
		 /* FileReader f1=new FileReader("C:\\Users\\double lancer\\Desktop\\exame.csv");
			BufferedReader reader =new BufferedReader(f1);*/
		  //reader.readLine();//第一行信息，为标题信息，不用，如果需要，注释掉
          String line = null;  
          while((line=reader.readLine())!=null){   // 
              String item[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
              
             // group.add(new data(item));  //第二种构造方法
              
              group.add(new data(item[0],   //这里采用了第一种构造方法
            		  Integer.parseInt(item[1]),
            		  Integer.parseInt(item[2]),
            		  item[3],Float.parseFloat(item[4]),
            		  Float.parseFloat(item[5]),
            		  Float.parseFloat(item[6]),
            		  Float.parseFloat(item[7]),
            		  Float.parseFloat(item[8]),
            		  Float.parseFloat(item[9]),
            		  Float.parseFloat(item[10]),
            		  Float.parseFloat(item[11]),
            		  Float.parseFloat(item[12]),
            		  Float.parseFloat(item[13]),
            		  Float.parseFloat(item[14]),
            		  Float.parseFloat(item[15]),
            		  Float.parseFloat(item[16]),
            		  Float.parseFloat(item[17]),
            		  Float.parseFloat(item[18]),
            		  Integer.parseInt(item[19]),
            		  Integer.parseInt(item[20]),
            		  Integer.parseInt(item[21]),
            		  Integer.parseInt(item[22]),
            		  Integer.parseInt(item[23]),
            		  Integer.parseInt(item[24]),
            		  Integer.parseInt(item[25]),
            		  Integer.parseInt(item[26]),
            		  Integer.parseInt(item[27]),
            		  Integer.parseInt(item[28]),
            		  Float.parseFloat(item[29]),
            		  Float.parseFloat(item[30]),
            		  Float.parseFloat(item[31]),
            		  Float.parseFloat(item[32]),
            		  Float.parseFloat(item[33]),
            		  Float.parseFloat(item[34]),
            		  Float.parseFloat(item[35]),
            		  Float.parseFloat(item[36]),
            		  Float.parseFloat(item[37]),
            		  Float.parseFloat(item[38]),
            		  Integer.parseInt(item[39]),
            		  Integer.parseInt(item[40]),
            		  Integer.parseInt(item[41]),
            		  Integer.parseInt(item[42]),
            		  Integer.parseInt(item[43]),
            		  Integer.parseInt(item[44]),
            		  Integer.parseInt(item[45]),
            		  Integer.parseInt(item[46]),
            		  Integer.parseInt(item[47]),
            		  Integer.parseInt(item[48]),
            		  Float.parseFloat(item[49]),
            		  Long.parseLong(item[50]),
            		  Double.parseDouble(item[51]),
            		  Long.parseLong(item[52]),
            		  Long.parseLong(item[53]),
            		  Float.parseFloat(item[54]),
            		  Float.parseFloat(item[55]),
            		  Float.parseFloat(item[56]),
            		  Float.parseFloat(item[57]),
            		  Float.parseFloat(item[58]),
            		  Float.parseFloat(item[59]),
            		  Float.parseFloat(item[60]),
            		  Long.parseLong(item[61]),
            		  Float.parseFloat(item[62]),
            		  Float.parseFloat(item[63])));   
          }/**/
          reader.close(); 
		}
	  
	  
	  
	  public void codesort() throws IOException{ //将数据按code排序，存入codegroup中
		  codegroup.add((data)group.get(0));
		  for(int i=0;i<group.size();i++) {
				for(int j=i+1;j<group.size();j++) {
					if(((data)group.get(i)).code==((data)group.get(j)).code) {
						group.add((data)group.get(j));
					}
				}
				
				for(int k=i+1;k<group.size();k++) {
					if(((data)group.get(i)).code!=((data)group.get(k)).code) {
						group.add((data)group.get(k));
						break;
					}
				}
				
			}
		  
		 /* for(int i=0;i<group.size()-1;i++) {
					if(((data)group.get(i)).code==((data)group.get(i+1)).code) {
						group.remove(i+1);
				}
			}	*/
	} 
	  
	  public void volumeDeal() throws IOException{
		  for(int i=0;i<(codegroup.size()-1);i++) {
			  if(((data)codegroup.get(i)).code==((data)codegroup.get(i+1)).code) {
					if(((data)codegroup.get(i)).Volume>=((data)codegroup.get(i+1)).Volume) {
						
						/*for(int k=0;k<i+1;k++) {  //将其插入正确的位置
							if((((data)codegroup.get(i+1)).Volume<((data)codegroup.get(k)).Volume)&&(((data)codegroup.get(k)).code==((data)codegroup.get(i+1)).code))
								codegroup.add(k,(data)codegroup.get(i+1));
						}*/
						codegroup.remove(i+1);//暂时使用移除,作为处理方法
						
						//
				}
			  }
			  else {
				  continue;
			  }
		}	
	}
	  public void timeDeal() throws IOException{
		  for(int i=0;i<codegroup.size()-1;i++) {
			  if(((data)codegroup.get(i)).code==((data)codegroup.get(i+1)).code) {
					if(((data)codegroup.get(i)).Time>=((data)codegroup.get(i+1)).Time) {
						
						/*for(int k=0;k<i+1;k++) {  //将其插入正确的位置
							if((((data)codegroup.get(i+1)).Time<((data)codegroup.get(k)).Time)&&(((data)codegroup.get(k)).code==((data)codegroup.get(i+1)).code))
								codegroup.add(k,(data)codegroup.get(i+1));
						}*/
						//codegroup.remove(i+1);
				}
			  }
			  else {
				  continue;
			  }
		  }
	}
	  public void highDeal() throws IOException{
		  for(int i=0;i<codegroup.size()-1;i++) {
			  if(((data)codegroup.get(i)).code==((data)codegroup.get(i+1)).code) {
					if(((data)codegroup.get(i)).High>=((data)codegroup.get(i+1)).High) {
						/*for(int k=0;k<i+1;k++) {  //将其插入正确的位置
							if((((data)codegroup.get(i+1)).High<((data)codegroup.get(k)).High)&&(((data)codegroup.get(k)).code==((data)codegroup.get(i+1)).code))
								codegroup.add(k,(data)codegroup.get(i+1));
						}*/
						codegroup.remove(i+1);
				}
			  }
			  else {
				  continue;
			  }
		  }
	}
	  public void lowDeal() throws IOException{
		  for(int i=0;i<codegroup.size()-1;i++) {
			  if(((data)codegroup.get(i)).code==((data)codegroup.get(i+1)).code) {
					if(((data)codegroup.get(i)).Low<=((data)codegroup.get(i+1)).Low) {
						/*for(int k=0;k<i+1;k++) {  //将其插入正确的位置
							if((((data)codegroup.get(i+1)).Low>((data)codegroup.get(k)).Low)&&(((data)codegroup.get(k)).code==((data)codegroup.get(i+1)).code))
								codegroup.add(k,(data)codegroup.get(i+1));
						}*/
						codegroup.remove(i+1);
				}
			  }
			  else {
				  continue;
			  }
		  }
	}
	  
	  public void LimitedDeal() throws IOException{
		  for(int i=0;i<codegroup.size();i++) {
					if(((data)codegroup.get(i)).HighLimited<0||((data)codegroup.get(i)).LowLimited<0) {
						codegroup.remove(i);
				}
			}	
	}
	  
	  public void  time_intervalDeal() throws IOException{
		  for(int i=0;i<codegroup.size()-1;i++) {
					if(((data)codegroup.get(i+1)).Time-((data)codegroup.get(i)).Time>=30) {
						writejournal(((data)codegroup.get(i+1)).market,((data)codegroup.get(i+1)).code,((data)codegroup.get(i+1)).Time,"shujvqueshi");
				}
			}	
	}
	  
	  public void  writejournal(String market,int code,int Time,String msg) throws IOException{   //写日志函数 ，参数Time，code 。。。。。。
		  FileWriter f=new FileWriter("C:\\Users\\double lancer\\Desktop\\journal.csv", true);  //加true 附加
			BufferedWriter writerj =new BufferedWriter(f);
			writerj.append(market+",");
			writerj.append(code+",");
			writerj.append(Time+",");
			writerj.append(msg+"\n");
			writerj.flush();
	  }
	  
	  public void writeData() throws IOException {
			FileWriter f=new FileWriter("C:\\Users\\double lancer\\Desktop\\newdata.csv", true);  //加true 附加
			BufferedWriter writererrs =new BufferedWriter(f);
			for(int i=0;i<group.size();i++) {
				writererrs.append(i+",");
				writererrs.append(((data)group.get(i)).Time+",");
				writererrs.append(((data)group.get(i)).Volume+",");
				writererrs.append(((data)group.get(i)).HighLimited+",");
				writererrs.append(((data)group.get(i)).LowLimited+"\n");
				writererrs.flush();
				}
			writererrs.close();
		}
}



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
    
    //这里提供两种 前者准确性好安全性高但比较麻烦    第二种 安全性差，但使用方便
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

    data(String item[]){
    	this.market=item[0];
    	this.code =  Integer.parseInt(item[1]);   
    	this.Time=  Integer.parseInt(item[2]);
    	this.status= item[3];
    	this.PreClose= Float.parseFloat(item[4]);
    	this.Open= Float.parseFloat(item[5]);
    	this.High= Float.parseFloat(item[6]);
    	this.Low= Float.parseFloat(item[7]);
    	this.newPrice= Float.parseFloat(item[8]);
    	for(int i=0;i<10;i++)
    	{
    	 this.AskPrice[i]=Float.parseFloat(item[9+i]);
    	};
    	
    	for(int i=0;i<10;i++)
    	{
    	 this.AskVolume[i]=Integer.parseInt(item[19+i]);
    	};
    	
    	for(int i=0;i<10;i++)
    	{
    	 this.BidPrice[i]=Float.parseFloat(item[29+i]);
    	};
    	
    	for(int i=0;i<10;i++)
    	{
    	 this.AskVolume[i]=Integer.parseInt(item[39+i]);
    	};
    	
    	
    	this.matchItems= Float.parseFloat(item[49]);
    	this.Volume= Long.parseLong(item[50]);
    	this.Turnover= Double.parseDouble(item[51]);
    	this.TotalBidVolume= Long.parseLong(item[52]);
    	this.TotalAskVolume= Long.parseLong(item[53]);
    	this.IOPV=  Float.parseFloat(item[54]);
    	this.HighLimited= Float.parseFloat(item[55]);
    	this.LowLimited= Float.parseFloat(item[56]);
    	this.afterPrice=  Float.parseFloat(item[57]);
    	this.afterVolume= Float.parseFloat(item[58]);
    	this.afterTurnover= Float.parseFloat(item[59]);
    	this.afterMatchItems= Float.parseFloat(item[60]);
    	this.LocalTime= Long.parseLong(item[61]);
    	this.weightedAvgAskPx= Float.parseFloat(item[62]);
    	this.weightedAvgBidPx= Float.parseFloat(item[63]);
    	
    	
    }
}
