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
    //private Boolean data_error = false;
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
                System.out.println("date : "+date+"'s data is error!\n");
                return true;
            }
            if((buff = reader.readLine())==null){
                System.out.println("date : "+date+" is null!\n");
                return true;
            }

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
                    Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),
                    Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),Float.parseFloat(item[56]),
                    Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),
                    Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63]));
            if(d.market == "SZ") {
                String temp_s = d.LocalTime + "";
                temp_s = temp_s.substring(10, 14);
                last_data_time_marketsz = Integer.parseInt(temp_s);
            }
            else if(d.market == "SH") {
                String temp_s = d.LocalTime + "";
                temp_s = temp_s.substring(10, 14);
                last_data_time_marketsh = Integer.parseInt(temp_s);
            }
            String temps=d.LocalTime+"";
            temps = temps.substring(8,14);
            int templ = Integer.parseInt(temps);
            open_time = templ+100;
            put_to_map(d);

            while((strTemp = reader.readLine()) != null) {
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
                        Float.parseFloat(item[49]),Long.parseLong(item[50]),Double.parseDouble(item[51]),Long.parseLong(item[52]),
                        Long.parseLong(item[53]),Float.parseFloat(item[54]),Float.parseFloat(item[55]),Float.parseFloat(item[56]),
                        Float.parseFloat(item[57]),Float.parseFloat(item[58]),Float.parseFloat(item[59]),Float.parseFloat(item[60]),
                        Long.parseLong(item[61]),Float.parseFloat(item[62]),Float.parseFloat(item[63]));
                }catch(Exception e){
                        e.printStackTrace();
                }
                if(d.market == "SZ") {
                    String temp_s = d.LocalTime + "";
                    temp_s = temp_s.substring(10, 14);
                    int curr_time = Integer.parseInt(temp_s);
                    inter_more_than_30s(d,curr_time,last_data_time_marketsz);
                    last_data_time_marketsz = curr_time;
                }
                else if(d.market == "SH") {
                    String temp_s = d.LocalTime + "";
                    temp_s = temp_s.substring(10, 14);
                    int curr_time = Integer.parseInt(temp_s);
                    inter_more_than_30s(d,curr_time,last_data_time_marketsh);
                    last_data_time_marketsh = curr_time;
                }
                put_to_map(d);
            }
            reader.close();
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
        if((curr_time%100) == (last_time%100)) {
            if((curr_time - last_time)>30) {
                long_interval_error++;
                bw.write("error 10 : Lost data, above data: "+d.market + d.code + d.LocalTime+"\n");
            }
        }
        else {
              int one = last_time%10;
            int ten = (last_time/10)%10;
            int seconds = ten * 10 + one + 30;
            if(seconds>=60) {
                last_time = (last_time%100 + 1)*100 + seconds - 60;
                if(curr_time>last_time) {
                    long_interval_error++;
                    bw.write("error 10 : Lost data, above data: "+d.market + d.code + d.LocalTime+"\n");
                }
            }
            else {
                long_interval_error++;
                bw.write("error 10 : Lost data, above data: "+d.market + d.code + d.LocalTime+"\n");
            }
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
            try{
                if (AskPriceFalse) bw.write("error 05: BidPrice error: Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
                else bw.write("error 05: AskPrice error: Market:"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
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
                bw.write("error 04 : AskPrice1 $ BidPrice1 error: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
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
                bw.write("error 06: Price is 0,but volume isn't 0: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            if(d.newPrice>d.HighLimited) {
                newprice_error++;
                try{
                    bw.write("error 06: Price more than HighLimited: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(d.newPrice<d.LowLimited) {
                newprice_error++;
                try{
                    bw.write("error 06: Price less than LowLimited: Market"+d.market+" Code:"+d.code+" Time:"+d.LocalTime+"\n");
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
                bw.write("error 03: Limited error: "+d.market+d.code+d.LocalTime+"\n");
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
        data next_d;
        data d;
        Iterator<Map.Entry<String, LinkedList<data>>> iterator_map = map.entrySet().iterator();
        while (iterator_map.hasNext()) {
            Map.Entry<String, LinkedList<data>> entry = iterator_map.next();
            String key = entry.getKey();
            LinkedList<data> lst = entry.getValue();
            ListIterator<data> iterator_list = lst.listIterator();
            d = iterator_list.next();
            while(iterator_list.hasNext()) {
                next_d = iterator_list.next();
                try{
                    limited_deal(d);
                    ask1_bid1_deal(d);
                    price_deal(d);
                    last_price_deal(d);
                    if(d.Time>next_d.Time) {
                        time_error++;
                        bw.write("error 01: Time increase error: " + d.market + d.code + d.LocalTime+"\n");
                    }
                    if(d.Volume>next_d.Volume) {
                        volume_error++;
                        bw.write("error 02: Volume increase error: " + d.market + d.code + d.LocalTime+"\n");
                    }
                    if(d.Turnover>next_d.Turnover) {
                        turnover_error++;
                        iterator_list.previous();
                        iterator_list.remove();
                        if(iterator_list.hasNext()){
                                iterator_list.next();
                        }
                        bw.write("error 07: Turnover increase error: " + d.market + d.code + d.LocalTime+"\n");
                    }
                    if(d.High>next_d.High) {
                        high_error++;
                        bw.write("error 08: High increase error: " + d.market + d.code + d.LocalTime+"\n");
                    }
                    if(d.Low<next_d.Low) {
                        low_error++;
                        bw.write("error 09: Low increase error: " + d.market + d.code + d.LocalTime+"\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                d = next_d;
            }
            try{
                limited_deal(d);
                ask1_bid1_deal(d);
                price_deal(d);
                last_price_deal(d);
                map.replace(key, lst);
            } catch (Exception e) {
                e.printStackTrace();
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
class outTask  {
    public outTask(Map <String,LinkedList<data>> map) {
        this.finalData = map;
    }
    public void run_outTask(String date) {
        String FILENAME = "/home/gengmx/data/h5/"+date+".h5";
