package com.pixelengine.controller;

import com.google.gson.Gson;
import com.pixelengine.DataModel.JOfftaskOrderMsg;
import com.pixelengine.DataModel.JOfftaskOrderMsgQueue;

import com.pixelengine.DataModel.WConfig;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class OfftaskReceiver extends Thread {
    public void run() {
        System.out.println("OfftaskReceiver start running ...");
        try(ZContext context = new ZContext()){
            ZMQ.Socket socket = context.createSocket(SocketType.PULL) ;
            String socketStr = WConfig.getSharedInstance().offtask_export_producer ;
            socket.connect(socketStr) ;
            while(!Thread.currentThread().isInterrupted()){
                byte[] data = socket.recv(0) ;
                String jsonData = new String(data, ZMQ.CHARSET) ;
                Gson gson = new Gson() ;
                JOfftaskOrderMsg msg = gson.fromJson(jsonData, JOfftaskOrderMsg.class) ;
                //放入队列
                System.out.println("worker receive a order:"+msg.ofid);
                JOfftaskOrderMsgQueue.getSharedInstance().append(msg) ;
            }

        }catch (Exception ex ){
            System.out.println("OfftaskReceiver exception:"+ex.getMessage() );
            System.out.println("receiver is out.") ;
        }
    }
}
