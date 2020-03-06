/*
 * hexo 博客
 * 实现统计半小时内的数据
 */

package com.will;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
 * time: 20200307
 * functionality: collect events of target time range
 * author: will
 * email: zcenao21@gmail.com
 */
public class UDAFTimeRangeCollect extends AbstractGenericUDAFResolver {
    static final Logger LOG = LoggerFactory.getLogger(UDAFTimeRangeCollect.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        //judge parameter number, if not equals to 3, then throw exception
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Three argument is excepted.");
        }

        //judge parameter type. They should be long, string, string
        ObjectInspector oiKey = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        ObjectInspector oiValue = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[1]);
        ObjectInspector oiTimeStr = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[2]);
        if (oiKey.getCategory() != ObjectInspector.Category.PRIMITIVE
            || oiValue.getCategory() != ObjectInspector.Category.PRIMITIVE
            || oiTimeStr.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE");
        }

        PrimitiveObjectInspector inputKeyOI = (PrimitiveObjectInspector) oiKey;
        PrimitiveObjectInspector inputValueOI = (PrimitiveObjectInspector) oiValue;
        PrimitiveObjectInspector inputTimeStrOI = (PrimitiveObjectInspector) oiValue;
        if (inputKeyOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            throw new UDFArgumentTypeException(0, "Argument must be String, but"
                    + inputKeyOI.getPrimitiveCategory().name()
                    + " was passed.");
        }
        if (inputValueOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Argument must be Timstamp, but "
                    + inputValueOI.getPrimitiveCategory().name()
                    + " was passed.");
        }
        if (inputTimeStrOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Argument must be Timstamp, but "
                    + inputTimeStrOI.getPrimitiveCategory().name()
                    + " was passed.");
        }

        return new AllActionsDuringTimeRange();
    }

    public static class AllActionsDuringTimeRange extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputTimeStampOI;
        private PrimitiveObjectInspector inputActionOI;
        private PrimitiveObjectInspector inputTimeOI;
        private ListObjectInspector lOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException{
            super.init(m, parameters);

            //input:
            //for partial1 and complete stage, parameter should contains three
            //elements. For partial2 and final, we use string list
            if (m == Mode.PARTIAL1||m==Mode.COMPLETE) {
                assert(parameters.length == 3);
                inputTimeStampOI = (PrimitiveObjectInspector) parameters[0];
                inputActionOI = (PrimitiveObjectInspector) parameters[1];
                inputTimeOI = (PrimitiveObjectInspector) parameters[2];
            }else{
                lOI=(ListObjectInspector)parameters[0];
            }

            //output:
            //string list. For timestamp input, we first transform it as string
            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        static class MKArrayAggregationBuffer extends AbstractAggregationBuffer {
            TimeArrayModel timeArrayModel;
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MKArrayAggregationBuffer ret = new MKArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        public void reset(AggregationBuffer agg) throws HiveException {
            MKArrayAggregationBuffer myAgg=(MKArrayAggregationBuffer) agg;
            myAgg.timeArrayModel=new TimeArrayModel();
            myAgg.timeArrayModel.reset();
        }

        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 3);
            //all parameters should not be null
            if (parameters[0] == null || parameters[1] == null ||parameters[2] == null) {
                return;
            }
            MKArrayAggregationBuffer myagg = (MKArrayAggregationBuffer) agg;
            //if not inited yet, then set it
            if(!myagg.timeArrayModel.isInit()){
                Text timeString = new Text(PrimitiveObjectInspectorUtils.getString(parameters[2], inputTimeOI));
                if(timeString.getLength()<1) {
                    throw new HiveException(getClass().getSimpleName() + " wrong time format of the third parameter"
                            + " : " + timeString);
                }
                myagg.timeArrayModel.allocate(timeString);
            }

            String timeStamp = String.valueOf(PrimitiveObjectInspectorUtils.getLong(parameters[0], inputTimeStampOI));
            String action = PrimitiveObjectInspectorUtils.getString(parameters[1], inputActionOI);

            myagg.timeArrayModel.add(timeStamp,action);
        }

        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MKArrayAggregationBuffer myagg = (MKArrayAggregationBuffer) agg;
            return myagg.timeArrayModel.serialize();
        }

        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if(partial==null){return;}
            MKArrayAggregationBuffer my_agg = (MKArrayAggregationBuffer) agg;

            List<String> partialAgg = (List<String>) lOI.getList(partial);
            StringObjectInspector doi = (StringObjectInspector)lOI.getListElementObjectInspector();
            my_agg.timeArrayModel.merge(partialAgg,doi);
        }

        public Object terminate(AggregationBuffer agg) throws HiveException {
            MKArrayAggregationBuffer my_agg = (MKArrayAggregationBuffer) agg;
            if(my_agg.timeArrayModel.getTime()==null){
                return null;
            }

            DateTime mytime=new DateTime(my_agg.timeArrayModel.getTime().toString());

            Map map = new HashMap(my_agg.timeArrayModel.getContainer().size());
            map.putAll(my_agg.timeArrayModel.getContainer());

            List<Map.Entry<Text, Text>> listData = Lists.newArrayList(map.entrySet());
            ArrayList<Text> result = new ArrayList<Text>();
            result.add(my_agg.timeArrayModel.getTime());

            //time range logic
            for (Map.Entry<Text, Text> entry : listData) {
                Long timestamp=Long.parseLong(entry.getKey().toString());
                Long timeInterval = (mytime.getMillis() - timestamp) / (3600000*24);
                if (Math.abs(timeInterval) <= 10) {
                    result.add(entry.getValue());
                }
            }

            return result;
        }
    }
}