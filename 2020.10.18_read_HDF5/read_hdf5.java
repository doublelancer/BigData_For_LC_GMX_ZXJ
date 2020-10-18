
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5O_token_t;

public class read_hdf5 {
    public static void main(String args[]){
        sensor.ReadDataset("F:\\Java\\File_monitoring\\src\\data.h5");
    }
}

class sensor{
    static String PATH = "/";

    // read all markets and codes file under the current date
    private static String[] do_iterate(String FILENAME) {
        long file_id = -1;

        // Open a file using default properties.
        try {
            file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Begin iteration.
        System.out.println("Objects in root group:");
        try {
            if (file_id >= 0) {
                int count = (int) H5.H5Gn_members(file_id, PATH);
                String[] oname = new String[count];
                int[] otype = new int[count];
                int[] ltype = new int[count];
                H5O_token_t[] orefs = new H5O_token_t[count];
                H5.H5Gget_obj_info_all(file_id, PATH, oname, otype, ltype, orefs, HDF5Constants.H5_INDEX_NAME);
                try {
                    if (file_id >= 0)
                        H5.H5Fclose(file_id);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                return oname;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Close the file.
        try {
            if (file_id >= 0)
                H5.H5Fclose(file_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void ReadDataset(String FILENAME) {
        long file_id = -1;
        long group_id = -1;
        String[] groupName= {};
        String GROUPNAME;
        /*String dsetNAME [] = ;*/
        //遍历.h5文件下边的所有数据集
        groupName= sensor.do_iterate(FILENAME);
//        System.out.println("数组大小为："+dsetName.length);

        //遍历得到文件下边的各个数据集  Open an existing dataset.
        for(int i=0;i<groupName.length;i++) {
            // Open an existing file.
            try {
                file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            GROUPNAME = groupName[i];
            data_g dataG = new data_g();
//        	System.out.println("================="+DATASETNAME);
            try {
                if (file_id >= 0)
                    group_id = H5.H5Gopen(file_id, GROUPNAME, HDF5Constants.H5P_DEFAULT);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
/*          在此处通过调用读数据的函数可以取出相应的数据，并可以进行相应操作
//          数据存储时的类型可分为：int、long、double、float
//          分别对应4个函数：read_int、read_long、read_float、read_double
//          根据需要读取的数据类型调用相应函数
//          函数返回一个一维数组，数组里存放同一个标签的数据，如：time、volume、LocalTime等等
//          以下两个例子读取了h5文件中存储的time、volume数据，调用函数格式如下
//          函数共两个参数其中group_id不用修改，第二个参数是一个String类型参数，是自己要读取的数据
//          所有可读取的数据列在下边，都是可读取的，数据类型看level2数据存储设计说明书里的数据
//          uint32就是int、uint64就是long、float32就是float、float64就是double

//          {"time","status","PerClose","open","high","low","NewPrice","volume","turnover",
//            "AskPrice","BidPrice","AskVolume","BidVolume","AskAvgPrice","BidAvgPrice",
//            "TotalAskVolume","TotalBidVolume","HighLimited","LowLimited","LocalTime"}

 */

            dataG.time = read_int(group_id, "time");
            dataG.volume = read_long(group_id, "volume");

            // Operate the required actions
            for(int j=0;j<dataG.time.length;j++){
                System.out.println(dataG.time[j]+" "+dataG.volume[j]);
            }


//  *       操作结束
            //  Operation end

            // Close the file.
            try {
                if (file_id >= 0)
                    H5.H5Fclose(file_id);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    private static int[] read_int(long groupID, String dset){
        long dataset_id = -1;
        long dataspace_id = -1;
        int[] buff;
        long []dims = {0,0};
        int l;
        // get dataset
        try {
            dataset_id = H5.H5Dopen(groupID, dset, HDF5Constants.H5P_DEFAULT);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Get dataspace
        try {
            if (dataset_id >= 0) {
                dataspace_id = H5.H5Dget_space(dataset_id);
                H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        l = (int)dims[0];
        buff = new int[l];

        try {
            if (dataspace_id >= 0){
                H5.H5Dread(dataset_id,HDF5Constants.H5T_NATIVE_UINT32,HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT,buff);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buff;
    }

    private static long[] read_long(long groupID, String dset){
        long dataset_id = -1;
        long dataspace_id = -1;
        long[] buff;
        long []dims = {0,0};
        int l;
        // get dataset
        try {
            dataset_id = H5.H5Dopen(groupID, dset, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get dataspace
        try {
            if (dataset_id >= 0) {
                dataspace_id = H5.H5Dget_space(dataset_id);
                H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        l = (int)dims[0];
        buff = new long[l];

        try {
            if (dataspace_id >= 0){
                H5.H5Dread(dataset_id,HDF5Constants.H5T_NATIVE_UINT64,HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT,buff);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buff;
    }

    private static float[] read_float(long groupID, String dset){
        long dataset_id = -1;
        long dataspace_id = -1;
        float[] buff;
        long []dims = {0,0};
        int l;
        // get dataset
        try {
            dataset_id = H5.H5Dopen(groupID, dset, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get dataspace
        try {
            if (dataset_id >= 0){
                dataspace_id = H5.H5Dget_space(dataset_id);
                H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        l = (int)dims[0];
        buff = new float[l];
        try {
            if (dataspace_id >= 0){
                H5.H5Dread(dataset_id,HDF5Constants.H5T_NATIVE_FLOAT,HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT,buff);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buff;
    }

    private static double[] read_double(long groupID, String dset){
        long dataset_id = -1;
        long dataspace_id = -1;
        double[] buff;
        long []dims = {0,0};
        int l;
        // get dataset
        try {
            dataset_id = H5.H5Dopen(groupID, dset, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get dataspace
        try {
            if (dataset_id >= 0){
                dataspace_id = H5.H5Dget_space(dataset_id);
                H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        l = (int)dims[0];
        buff = new double[l];
        try {
            if (dataspace_id >= 0){
                H5.H5Dread(dataset_id,HDF5Constants.H5T_NATIVE_DOUBLE,HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT,buff);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buff;
    }
}
class data_g{
    int []time;
    int []status;
    float []perClose;
    float []open;
    float []high;
    float []low;
    float []newPrice;
    long []volume;
    double []turnover;
    float [][]AskPrice;
    float [][]BidPrice;
    int [][]AskVolume;
    int [][]BidVolume;
    float []AskAvgPrice;
    float []BidAvgPrice;
    long []TotalAskVolume;
    long []TotalBidVolume;
    long []LocalTime;
    float []HighLimited;
    float []LowLimited;
}





















