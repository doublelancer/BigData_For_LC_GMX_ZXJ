import java.io.*;
import java.util.*;
import java.lang.*;

public class WriteH5 {
    public static void main(String []args){
        String file = "F:\\Java\\Write2H5\\data\\data.txt";
        String FILENAME = "F:\\Java\\Write2H5\\data\\data.h5";
        Process process = new Process();
        process.read_data(file);

        Write2H5 wh = new Write2H5(process.map);
        wh.write(FILENAME);
    }
}

class Process{
    public void read_data(String filename) {
        try {

            BufferedReader reader = null;
            String buff;
            String []item;
            data d = null;
            reader = new BufferedReader(new FileReader(new File(filename)));

            while((buff = reader.readLine()) != null) {
                item = buff.split(",");
                try{
                    d = new data(item[0],item[1],Integer.parseInt(item[2]),Integer.parseInt(item[3]),Float.parseFloat(item[4]),
                            Float.parseFloat(item[5]),Float.parseFloat(item[6]),Float.parseFloat(item[7]),Float.parseFloat(item[8]),
                            Float.parseFloat(item[9]),Float.parseFloat(item[10]),Float.parseFloat(item[11]),Float.parseFloat(item[12]),
                            Float.parseFloat(item[13]),Float.parseFloat(item[14]),Float.parseFloat(item[15]),Float.parseFloat(item[16]),
                            Float.parseFloat(item[17]),Float.parseFloat(item[18]),Integer.parseInt(item[19]),Integer.parseInt(item[20]),
                            Integer.parseInt(item[21]),Integer.parseInt(item[22]),Integer.parseInt(item[23]),Integer.parseInt(item[24]),
                            Integer.parseInt(item[25]),Integer.parseInt(item[26]),Integer.parseInt(item[27]),Integer.parseInt(item[28]),
                            Float.parseFloat(item[29]),Float.parseFloat(item[30]),Float.parseFloat(item[31]),Float.parseFloat(item[32]),
                            Float.parseFloat(item[33]),Float.parseFloat(item[34]),Float.parseFloat(item[35]),Float.parseFloat(item[36]),
                            Float.parseFloat(item[37]),Float.parseFloat(item[38]),Integer.parseInt(item[39]),Integer.parseInt(item[40]),
                            Integer.parseInt(item[41]),Integer.parseInt(item[42]),Integer.parseInt(item[43]),Integer.parseInt(item[44]),
                            Integer.parseInt(item[45]),Integer.parseInt(item[46]),Integer.parseInt(item[47]),Integer.parseInt(item[48]),
                            Float.parseFloat(item[49]),Long.parseLong(item[50]),Float.parseFloat(item[51]),Long.parseLong(item[52]),
                            Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),Float.parseFloat(item[56]),
                            Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),
                            Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63]));
                }catch(Exception e){
                    e.printStackTrace();
                }
                put_to_map(d);
            }
            reader.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    private static void put_to_map(data d) {
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
    public static Map<String,LinkedList<data>> map = new HashMap<String, LinkedList<data>>();
}

class data implements Cloneable{
    String market;
    String code;
    int Time;
    int status;
    float PreClose;
    float Open;
    float High;
    float Low;
    float newPrice;
    float[] AskPrice =new float[10];
    int[] AskVolume =new int[10];
    float[] BidPrice =new float[10];
    int[] BidVolume =new int[10];
    float matchItems;
    long Volume;
    float Turnover;
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
    data(String market,String code,int Time, int status, float PreClose,float Open,float High,float Low,float newPrice,
         float AskPrice0,float AskPrice1,float AskPrice2,float AskPrice3,float AskPrice4,float AskPrice5,float AskPrice6,float AskPrice7,float AskPrice8,float AskPrice9,int AskVolume0,int AskVolume1,int AskVolume2,int AskVolume3,int AskVolume4,int AskVolume5,int AskVolume6,int AskVolume7,int AskVolume8,int AskVolume9,float BidPrice0,float BidPrice1,float BidPrice2,float BidPrice3,float BidPrice4,float BidPrice5,float BidPrice6,float BidPrice7,float BidPrice8,float BidPrice9,int BidVolume0,int BidVolume1,int BidVolume2,int BidVolume3,int BidVolume4,int BidVolume5,int BidVolume6,int BidVolume7,int BidVolume8,int BidVolume9,float matchItems,long Volume,float Turnover,long TotalBidVolume,long TotalAskVolume,float IOPV,float HighLimited,float LowLimited,float afterPrice,float afterVolume,float afterTurnover,float afterMatchItems,long LocalTime,float weightedAvgAskPx,float weightedAvgBidPx){
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
    @Override
    public Object clone(){
        data d = null;
        try{
            d = (data)super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return d;
    }
}
