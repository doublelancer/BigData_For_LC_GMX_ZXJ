import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import java.util.*;
import java.lang.*;
class Write2H5  {
    public Write2H5(Map <String,LinkedList<data>> map) {
        this.finalData = map;
    }

    public void write(String FILENAME) {
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
