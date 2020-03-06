package com.will;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeArrayModel {
    private transient Text timeString;
    private Map<Object, Object> container;

    public TimeArrayModel(){
        timeString=new Text();
        container=null;
    }

    public boolean isInit(){
        if(timeString==null){
            return false;
        }else{
            return true;
        }
    }

    public Text getTime(){
        return timeString;
    }

    public void allocate(Text timeString){
        container=Maps.newHashMap();
        this.timeString=timeString;
    }

    public void reset(){
        container=null;
        timeString=null;
    }

    public void merge(List other, StringObjectInspector doi){
        if(other==null){return;}
        //未初始化
        if(timeString==null){
            timeString=doi.getPrimitiveWritableObject(other.get(0));
            container=new HashMap<>();
        }
        for (int i = 1; i < other.size(); i+=2) {
            container.put(other.get(i),other.get(i+1));
        }
    }

    public void add(Object o1,Object o2){
        container.put(o1,o2);
    }

    public Map<Object,Object> getContainer(){
        return container;
    }

    public ArrayList<Text> serialize() {
        ArrayList<Text> result = new ArrayList<Text>();

        // Return a single ArrayList where the first element is the number of bins bins,
        // and subsequent elements represent bins (x,y) pairs.
        result.add(timeString);
        if(timeString != null) {
            for (Map.Entry<Object, Object> entry : container.entrySet()) {
                Object kCopy = entry.getKey();
                Object vCopy = entry.getValue();
                result.add(new Text((String)kCopy));
                result.add(new Text((String)vCopy));
            }
        }

        return result;
    }
}
