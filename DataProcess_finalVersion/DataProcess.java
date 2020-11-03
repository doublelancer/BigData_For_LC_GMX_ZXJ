import java.io.*;
import java.util.*;
import java.lang.*;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class DataProcess{
    private static ArrayList<String> targzfile = new ArrayList<String>();

    public static void main(String[] args) {
        String path = "/home/data/level2/stk";
        getFile(path);
        long global_startTime = System.currentTimeMillis();
        for(int i = 0;i < targzfile.size();i ++){
                String cmd_tar = "tar -zxvf /home/data/level2/stk/"+targzfile.get(i)+" -C /home/gengmx/data/tar/";
                DataProcess.exec(cmd_tar);
                String date = targzfile.get(i).substring(0,8);
                //System.out.println("Processing data : "+date);        
                process processtask = new process();
                processtask.processData(date);
                String cmd_rm = "rm -r /home/gengmx/data/tar/"+targzfile.get(i).substring(0,8);
                DataProcess.exec(cmd_rm);
        }
        System.out.println("Number of all data : "+process.total);
        System.out.println("Number of right data : "+process.right);
        System.out.println("Number of error data : "+process.error);
        long global_endTime = System.currentTimeMillis();
        System.out.println("Total time spend is : "+(global_endTime-global_startTime)+" ms");
    }
    private static void getFile(String path) {
        File file = new File(path);
        File[] array = file.listFiles();
        for (int i = 0; i < array.length; i++) {
            if (array[i].isFile()) {
                if(array[i].getName().length() > 10)
                    targzfile.add(array[i].getName());
            } else if (array[i].isDirectory()) {
                getFile(array[i].getPath());
            }
        }
    }

    public static Object exec(String cmd) {
        try {
            String[] cmdA = {"/bin/sh", "-c", cmd};
            Process process = Runtime.getRuntime().exec(cmdA);
            LineNumberReader br = new LineNumberReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                sb.append(line).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
  }
  class process {
    public static int total=0;
    public static int error=0;
    public static int right=0;
    public void processData(String date) {
        long startTime = System.currentTimeMillis();
        try {
            process task1 = new process();
            outTask task3 = new outTask(map);
            Boolean data_error = false;
            data_error = task1.read_data(date);
            if(data_error){
                return;
            }
            task1.traverse_process();

            bw.close();

            task3.run_outTask(date);
        }catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        try{
            String count_filename = "/home/gengmx/data/logfile/countLog/"+date;
            BufferedWriter write_data_num = new BufferedWriter(new FileWriter(new File(count_filename)));
            write_data_num.write("Total data:"+data_num+"\n");
            write_data_num.write("Total time error(error 01):"+time_error+"\n");
            write_data_num.write("Total volume error(error 02):"+volume_error+"\n");
            write_data_num.write("Total limited error(error 03):"+limit_error+"\n");
            write_data_num.write("Total ask1&bid1 error(error 04):"+ask1_bid1_error+"\n");
            write_data_num.write("Total price error(error 05):"+price_error+"\n");
            write_data_num.write("Total new price error(error 06):"+newprice_error+"\n");
            write_data_num.write("Total turnover error(error 07):"+turnover_error+"\n");
            write_data_num.write("Total high error(error 08):"+high_error+"\n");
            write_data_num.write("Total low error(error 09):"+low_error+"\n");
            write_data_num.write("Total mote than 30s error(error 10):"+long_interval_error+"\n");
            write_data_num.write("处理时间：" + (endTime - startTime) + "ms"+"\n");
            write_data_num.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        map.clear();
        data_num = 0;
        time_error = 0;
        volume_error = 0;
        limit_error = 0;
        ask1_bid1_error = 0;
        price_error = 0;
        newprice_error = 0;
        turnover_error = 0;
        high_error = 0;
        low_error = 0;
        long_interval_error = 0;
    }
    private Boolean read_data(String date) {
        int last_data_time_marketsh = 0;
        int last_data_time_marketsz = 0;

        try {
            String filename = "/home/gengmx/data/tar/"+date+"/"+date;
            total++;
            //File file = new File(filename);
            BufferedReader reader = null;
            String strTemp;
            String []item;
            data d;
            reader = new BufferedReader(new FileReader(new File(filename)));
            System.out.println("Processing date : "+date+"'s data");
            String buff = reader.readLine();
            String []buff_array = buff.split(",");

            if(buff_array.length < 64){
                error++;
                System.out.println("date : "+date+"'s data is error!\n");
                return true;
            }
            if((buff = reader.readLine())==null){
                error++;
                System.out.println("date : "+date+" is null!\n");
                return true;
            }
            right++;

            String error_filename = "/home/gengmx/data/logfile/errorLog/"+date;
            bw = new BufferedWriter(new FileWriter(new File(error_filename)));

            data_num++;
            strTemp = reader.readLine();
            item = strTemp.split(",");
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
            if(d.market.equals("SZ")) {
                String temp_s = d.LocalTime + "";
                temp_s = temp_s.substring(10, 14);
                last_data_time_marketsz = Integer.parseInt(temp_s);
            }
            else if(d.market.equals("SH")) {
                String temp_s = d.LocalTime + "";
                temp_s = temp_s.substring(10, 14);
                last_data_time_marketsh = Integer.parseInt(temp_s);
            }
            String temps=d.LocalTime+"";
            temps = temps.substring(8,14);
            int templ = Integer.parseInt(temps);
            open_time = templ+100;
            put_to_map(d);

            String outFile = "/home/gengmx/data/detect/"+date;
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outFile)));

            while((buff = reader.readLine()) != null) {
                String buff1 = strTemp;
                strTemp = buff;
                if(buff1.equals(strTemp)){
                    writer.write("Duplicate data:\n"+strTemp+"\n"+buff1+"\n\n");
                    continue;
                }
                data_num++;
                item = strTemp.split(",");
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
                if(d.market.equals("SZ")) {
                    String temp_s = d.LocalTime + "";
                    temp_s = temp_s.substring(10, 14);
                    int curr_time = Integer.parseInt(temp_s);
                    if(last_data_time_marketsz==0){
                        last_data_time_marketsz=curr_time;
                    }
                    else{
                        inter_more_than_30s(d,curr_time,last_data_time_marketsz);
                        last_data_time_marketsz = curr_time;
                    }
                }
                else if(d.market.equals("SH")) {
                    String temp_s = d.LocalTime + "";
                    temp_s = temp_s.substring(10, 14);
                    int curr_time = Integer.parseInt(temp_s);
                    if(last_data_time_marketsh==0){
                        last_data_time_marketsh=curr_time;
                    }
                    else{
                        inter_more_than_30s(d,curr_time,last_data_time_marketsh);
                        last_data_time_marketsh = curr_time;
                    }
                }
                put_to_map(d);
            }
            reader.close();
            writer.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
        return false;
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
    private void inter_more_than_30s(data d, int curr_time, int last_time) throws Exception{
        int curr_hund = curr_time/100;
        int last_hund = last_time/100;
        if(curr_hund == last_hund) {
            if((curr_time - last_time)>30) {
                long_interval_error++;
                bw.write("error 10: Lost data, above data:\t\tmarket:"+d.market +"\tcode:"+d.code +"\ttime:"+d.LocalTime+"\n");
            }
        }
        else {
            int one = last_time%10;
            int ten = (last_time/10)%10;
            int seconds = ten * 10 + one + 30;
            if(seconds>=60) {
                last_time = (last_time/100 + 1)*100 + seconds - 60;
                if(curr_time>last_time) {
                    long_interval_error++;
                    bw.write("error 10: Lost data, above data:\t\tmarket:"+d.market+"\tcode:"+d.code +"\ttime:"+d.LocalTime+"\n");
                }
            }
            else {
                long_interval_error++;
                bw.write("error 10: Lost data, above data:\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
            }
        }
    }

    private void price_deal(data d) throws Exception{
        boolean AskPriceError = false;
        boolean BidPriceError = false;
        if(d.HighLimited==0 || d.LowLimited==0) {
            return;
        }
        else {
            for(int i=0;i<d.AskPrice.length;i++) {
                if(d.AskPrice[i]!=0) {
                    if(d.AskPrice[i]>d.HighLimited||d.AskPrice[i]<d.LowLimited) {
                        AskPriceError = true;
                        price_error++;
                        break;
                    }

                }
                if(d.BidPrice[i]!=0) {
                    if(d.BidPrice[i]>d.HighLimited||d.BidPrice[i]<d.LowLimited) {
                        price_error++;
                        BidPriceError = true;
                        break;
                    }

                }
            }
            try{
                if (AskPriceError) bw.write("error 05: BidPrice error:\t\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                if (BidPriceError) bw.write("error 05: AskPrice error:\t\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void ask1_bid1_deal(data d) throws Exception{
        if(d.AskPrice[0]==0&&d.BidPrice[0]==0) {
            String temps=d.LocalTime+"";
            temps = temps.substring(8,14);
            int templ = Integer.parseInt(temps);
            if(!((templ>=145600&&templ<=145800)||(templ<=open_time))) {
                ask1_bid1_error++;
                bw.write("error 04: AskPrice1 $ BidPrice1 error:\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
            }
        }
    }
    private void last_price_deal(data d) throws Exception{
        if(d.newPrice==0&&d.Volume==0) {
            return;
        }
        else if(d.newPrice==0&&d.Volume!=0) {
            newprice_error++;
            try{
                bw.write("error 06: Price is 0,but volume isn't 0:\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            if(d.newPrice>d.HighLimited) {
                newprice_error++;
                try{
                    bw.write("error 06: Price more than HighLimited:\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(d.newPrice<d.LowLimited) {
                newprice_error++;
                try{
                    bw.write("error 06: Price less than LowLimited:\t\tmarket:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private void limited_deal(data d) throws Exception{

        if (d.HighLimited<=0||d.LowLimited<=0) {
            limit_error++;
            try{
                bw.write("error 03: Limited error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void traverse_process() {
        int last_time;
        double last_turnover;
        float last_high;
        float last_low;
        long last_volume;
        data d;
        Iterator<Map.Entry<String, LinkedList<data>>> iterator_map = map.entrySet().iterator();
        while (iterator_map.hasNext()) {
            Map.Entry<String, LinkedList<data>> entry = iterator_map.next();
            String key = entry.getKey();
            LinkedList<data> lst = entry.getValue();
            ListIterator<data> iterator_list = lst.listIterator();
            d = iterator_list.next();
            try{
                limited_deal(d);
                ask1_bid1_deal(d);
                price_deal(d);
                last_price_deal(d);
                map.replace(key, lst);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while(iterator_list.hasNext()) {
                data last_d = (data)d.clone();
                d = iterator_list.next();
                try{
                    limited_deal(d);
                    ask1_bid1_deal(d);
                    price_deal(d);
                    last_price_deal(d);
                }catch (Exception e) {
                    e.printStackTrace();
                }
                try{
                    if(d.Time<last_d.Time) {
                        time_error++;
                        bw.write("error 01: Time increase error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                    }
                    if(d.Volume<last_d.Volume) {
                        volume_error++;
                        bw.write("error 02: Volume increase error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                    }
                    if(d.Turnover<last_d.Turnover) {
                        turnover_error++;
                        iterator_list.previous();
                        iterator_list.remove();
                        if(iterator_list.hasNext()){
                            iterator_list.next();
                        }
                        bw.write("error 07: Turnover increase error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                    }
                    if(d.High<last_d.High) {
                        high_error++;
                        bw.write("error 08: High increase error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                    }
                    if(d.Low>last_d.Low) {
                        low_error++;
                        bw.write("error 09: Low increase error:\t\t\t"+"market:"+d.market+"\tcode:"+d.code+"\ttime:"+d.LocalTime+"\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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
    private static BufferedWriter bw;
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
