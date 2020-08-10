package aef;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;







public class aef {
	Vector group=new Vector();
	//Vector ogroup=new Vector();
	
	  public static void main(String[] args) { 
          try { 
        	  aef aef1=new aef();
        	  aef1.readData();
        	  aef1.timeDeal();
        	  aef1.volumeDeal();
        	  aef1.LimitedDeal();
        	  aef1.writeData();
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
              group.add(new data(Integer.parseInt(item[1]),Float.parseFloat(item[2]),Float.parseFloat(item[3]),Float.parseFloat(item[4])));   
          }/**/

          reader.close(); 
		}
	  
	  public void volumeDeal() throws IOException{
		  for(int i=0;i<(group.size()-1);i++) {
					if(((data)group.get(i)).Volume>=((data)group.get(i+1)).Volume) {
						group.remove(i+1);
				}
			}	
	}
	  
	  public void timeDeal() throws IOException{
		  for(int i=0;i<group.size()-1;i++) {
					if(((data)group.get(i)).Time>=((data)group.get(i+1)).Time) {
						group.remove(i+1);
				}
			}	
	}
	  public void LimitedDeal() throws IOException{
		  for(int i=0;i<group.size();i++) {
					if(((data)group.get(i)).HighLimited<0||((data)group.get(i)).LowLimited<0) {
						group.remove(i);
				}
			}	
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
    int Time;   
    float Volume;   
    float HighLimited;   
    float LowLimited;    
    data(int i,float q, float s, float d){   
    	Time = i;   
        Volume = q;   
        HighLimited = s;   
        LowLimited = d;   
    }   
}
