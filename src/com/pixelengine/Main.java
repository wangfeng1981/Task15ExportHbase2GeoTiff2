package com.pixelengine;


import com.google.gson.Gson;
import com.pixelengine.DataModel.*;

import java.io.*;

import com.pixelengine.controller.OfftaskReceiver;
import org.gdal.gdal.gdal;


public class Main {


    public static void main(String[] args) throws IOException {
	// write your code here
        System.out.println("A program to export HBase tile data into geotiff. Task15...2 . 2022-2-12. by wangfeng_dq@piesat.cn") ;
        System.out.println("-Djava.library.path should be configed for gdal*.so and libHBasePeHelperCppConnector.so") ;
        System.out.println("v1.0.0 created 2022-2-12.") ;
        System.out.println("v1.1.0 0mq ok, script order ok. 2022-2-13.") ;
        System.out.println("v1.1.1 use JExportOrder replace JOrder. 2022-2-15.") ;
        System.out.println("v1.1.2 update new java files, 2022-4-5.") ;
        System.out.println("v1.1.2.1 update HBase...Helper 2022-6-9 commited") ;
        System.out.println("v1.1.2.2 update java files from task17 2022-7-3") ;
        System.out.println("v1.1.2.3 update java files from task17 for nearest datetimeobj 2022-7-8") ;
        System.out.println(" ") ;
        System.out.println("call:java -jar /some/dir/Task15ExportHbase2GeoTiff2.jar task17config.json") ;
        System.out.println("datatype: 1-byte,2-u16,3-i16,4-u32,5-i32,6-float,7-double.");//2021-4-16

        if( args.length==1 )
        {
            //ok
        }else{
            System.out.println("Error: args.length != 1 , args.length:"+args.length);
            System.exit(11);
            return  ;
        }

        String configFile = args[0] ;
        //String orderFile = args[1] ;

        System.out.println("configFile:"+configFile);//与 Task17公用config文件
        //System.out.println("orderFile:"+orderFile);

        //init WConfig
        System.out.println("init wconfig");
        WConfig.init(configFile);

        //init mysql
        System.out.println("init mysql");
        JRDBHelperForWebservice.init( WConfig.getSharedInstance().connstr ,
                WConfig.getSharedInstance().user ,
                WConfig.getSharedInstance().pwd );

        //init gdal
        System.out.println("gdal AllRegister");
        gdal.AllRegister();


        //Pe version
        System.out.println("                                                                           ");
        System.out.println("██████╗ ███████╗    ██╗   ██╗███████╗██████╗ ███████╗██╗ ██████╗ ███╗   ██╗");
        System.out.println("██╔══██╗██╔════╝    ██║   ██║██╔════╝██╔══██╗██╔════╝██║██╔═══██╗████╗  ██║");
        System.out.println("██████╔╝█████╗      ██║   ██║█████╗  ██████╔╝███████╗██║██║   ██║██╔██╗ ██║");
        System.out.println("██╔═══╝ ██╔══╝      ╚██╗ ██╔╝██╔══╝  ██╔══██╗╚════██║██║██║   ██║██║╚██╗██║");
        System.out.println("██║     ███████╗     ╚████╔╝ ███████╗██║  ██║███████║██║╚██████╔╝██║ ╚████║");
        System.out.println("╚═╝     ╚══════╝      ╚═══╝  ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝");
        System.out.println("                                                                           ");
        HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector();
        System.out.println("pe version: " + cc.GetVersion() );

        //startup task recerver
        OfftaskReceiver receiver = new OfftaskReceiver() ;
        receiver.start();

        //startup result sender
        JOfftaskWorkerResultSender.getSharedInstance() ;

        try{
            System.out.println("OfftaskReceiver running...") ;
            while(true){
                JOfftaskOrderMsg orderMsg = JOfftaskOrderMsgQueue.getSharedInstance().pop() ;
                if( orderMsg != null )
                {
                    JOfftaskWorkerResult res = new JOfftaskWorkerResult() ;
                    res.ofid = orderMsg.ofid ;
                    String orderfilepath = WConfig.getSharedInstance().pedir+orderMsg.orderRelFilepath ;
                    String orderText =  WTextFile.readFileAsString(orderfilepath) ;
                    if( orderText==null ){
                        System.out.println("Error: orderText is null") ;
                        res.state = 19 ;//bad orderText
                    }else{
                        //读取处理订单
                        System.out.println("read order information");
                        Gson gson = new Gson() ;
                        JExportOrder order = gson.fromJson(orderText, JExportOrder.class) ;
                        //start to process
                        System.out.println("start a worker for processing...");
                        WOrderWorker worker = new WOrderWorker() ;
                        WResult workerResult = worker.processOneOrder( WConfig.getSharedInstance() ,order) ;
                        String workerResultJsonText = gson.toJson( workerResult ) ;
                        //结果json写入存储
                        String orderResultJsonFile = WConfig.getSharedInstance().pedir + order.resultRelFilepath ;
                        System.out.println("write workerResultJsonText into file " + orderResultJsonFile );
                        com.pixelengine.tools.FileDirTool.writeToFile(orderResultJsonFile , workerResultJsonText );
                        res.resultRelFilepath = order.resultRelFilepath ;
                        res.state = workerResult.state ;
                    }
                    //结果json通过0mq发给task17服务
                    JOfftaskWorkerResultSender.getSharedInstance().send( res );
                }

                Thread.sleep(1000);
            }
        }catch (Exception ex){
            System.out.println("Task15ExportHbase2GeoTiff2 main thread exception:"+ex.getMessage());
        }


        System.out.println("Task15...2 done.") ;
        System.exit(0);
    }
}
