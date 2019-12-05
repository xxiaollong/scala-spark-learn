package com.example.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 分组排序求最大值
 *
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;
    private double price;

    public OrderBean(){super();}


    public void setAll(int order_id, double price){
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        if (this.getOrder_id() > o.getOrder_id()){
            return 1;
        }else if (this.getOrder_id() < o.getOrder_id()){
            return -1;
        }else {
            if (this.getPrice() > o.getPrice()){
                return -1;
            }else if (this.getPrice() < o.getPrice()){
                return 1;
            }else {
                return 0;
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return  order_id +"\t" + price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}


