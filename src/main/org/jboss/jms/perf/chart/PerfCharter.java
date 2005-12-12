/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.chart;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jboss.jms.perf.framework.data.Benchmark;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.Measurement;
import org.jboss.jms.perf.framework.persistence.JDBCPersistenceManager;
import org.jboss.jms.perf.framework.persistence.PersistenceManager;
import org.jboss.logging.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.sun.image.codec.jpeg.JPEGCodec;
import com.sun.image.codec.jpeg.JPEGImageEncoder;

/*
 * Charts the performance test results.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class PerfCharter
{
   // Constants -----------------------------------------------------

   private static final String OUTPUT_DIR = "perf-output";
   
   private static final String OUTPUT_FILE = "jms-perf-results.html";
   
   private static final Logger log = Logger.getLogger(PerfCharter.class);   
   
   
   // Static --------------------------------------------------------
   
   public static void main(String[] args)
   {
      new PerfCharter().run();
   }

   // Attributes ----------------------------------------------------
   
   protected PersistenceManager pm;
   
   protected File outputDir;
   
   protected Writer writer;
   

   // Constructors --------------------------------------------------
   
   private PerfCharter()
   {      
   }


   // Connection implementation -------------------------------------

   // Public -------------------------------------------------------- 
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------  
      
   protected void doCharts()
      throws Exception
   {            
      chartSimpleBenchmark("Queue1", "Date", "Send rate (messages/sec)",
            "Non transactional, Non-persistent send only of messages to queue", "Queue1.jpg");      
      
      chartSimpleBenchmark("Queue2", "Date", "Send rate (messages/sec)",
            "Non transactional, persistent send only of messages to queue", "Queue2.jpg"); 
      
      chartSimpleBenchmark("Queue3", "Date", "Send rate (messages/sec)",
            "Transactional, non-persistent send only of messages to queue", "Queue3.jpg");      
      
      chartSimpleBenchmark("Queue4", "Date", "Send rate (messages/sec)",
            "Transactional, persistent send only of messages to queue", "Queue4.jpg"); 
      
      chartSimpleBenchmark("Queue5", "Date", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive to queue, AUTO_ACKNOWLEDGE", "Queue5.jpg");  
      
      chartSimpleBenchmark("Queue6", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive to queue, AUTO_ACKNOWLEDGE", "Queue6.jpg");   
      
      chartSimpleBenchmark("Queue7", "Date", "Throughput (messages/sec)",
            "Transactional, non-persistent send and receive to queue", "Queue7.jpg");
      
      chartSimpleBenchmark("Queue8", "Date", "Throughput (messages/sec)",
            "Transactional, persistent send and receive to queue", "Queue8.jpg");
      
      chartSimpleBenchmark("Queue9", "Date", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive to queue, DUPS_OK_ACKNOWLEDGE", "Queue9.jpg");  
      
      chartSimpleBenchmark("Queue10", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive to queue, DUPS_OK_ACKNOWLEDGE", "Queue10.jpg"); 
   
      chartSimpleBenchmark("Queue11", "Date", "Receive rate (messages/sec)",
            "Non-transactional, persistent receive only from queue, AUTO_ACKNOWLEDGE", "Queue11.jpg"); 
      
      chartSimpleBenchmark("Queue12", "Date", "Receive rate (messages/sec)",
            "Non-transactional, non-persistent receive only from queue, AUTO_ACKNOWLEDGE", "Queue12.jpg"); 
      
      chartSimpleBenchmark("Queue13", "Date", "Receive rate (messages/sec)",
            "Transactional, persistent receive only from queue", "Queue13.jpg"); 
      
      chartSimpleBenchmark("Queue14", "Date", "Receive rate (messages/sec)",
            "Transactional, non-persistent receive only from queue", "Queue14.jpg");
      
      chartSimpleBenchmark("Queue15", "Date", "Receive rate (messages/sec)",
            "Non-transactional, persistent receive only from queue with selector, AUTO_ACKNOWLEDGE", "Queue15.jpg");
   
      chartSimpleBenchmark("Queue16", "Date", "Receive rate (messages/sec)",
            "Non-transactional, non-persistent receive only from queue with selector, AUTO_ACKNOWLEDGE", "Queue16.jpg");
   
      chartSimpleBenchmark("Queue17", "Date", "Browse rate (messages/sec)",
            "Browse persistent messages", "Queue17.jpg");
      
      chartSimpleBenchmark("Queue18", "Date", "Browse rate (messages/sec)",
            "Browse non-persistent messages", "Queue18.jpg");
      
      chartSimpleBenchmark("Queue19", "Date", "Browse rate (messages/sec)",
            "Browse non-persistent messages with selector", "Queue19.jpg");
      
      chartSimpleBenchmark("Topic1", "Date", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, one non-durable subscriber, AUTO_ACKNOWLEDGE", "Topic1.jpg");      
      
      chartSimpleBenchmark("Topic2", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive with topic, one non-durable subscriber, AUTO_ACKNOWLEDGE", "Topic2.jpg");
      
      chartSimpleBenchmark("Topic3", "Date", "Throughput (messages/sec)",
            "Transactional, non-persistent send and receive with topic, one non-durable subscriber", "Topic3.jpg");      
      
      chartSimpleBenchmark("Topic4", "Date", "Throughput (messages/sec)",
            "Transactional, persistent send and receive with topic, one non-durable subscriber", "Topic4.jpg");
    
      chartSimpleBenchmark("Topic5", "Date", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, one non-durable subscriber, DUPS_OK_ACKNOWLEDGE", "Topic5.jpg");      
     
      chartSimpleBenchmark("Topic6", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive with topic, one non-durable subscriber, DUPS_OK_ACKNOWLEDGE", "Topic6.jpg");      
      
      chartSimpleBenchmark("Topic7", "Date", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, one non-durable subscriber with selector, AUTO_ACKNOWLEDGE", "Topic7.jpg");      
    
      chartSimpleBenchmark("Topic8", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive with topic, one non-durable subscriber with selector, AUTO_ACKNOWLEDGE", "Topic8.jpg");      
      
      chartSimpleBenchmark("Topic9", "Date", "Throughput (messages/sec)",
            "Non-transactional, persistent send and receive with topic, one durable subscriber, AUTO_ACKNOWLEDGE", "Topic9.jpg");
     
      chartSimpleBenchmark("Topic10", "Date", "Throughput (messages/sec)",
            "Transactional, persistent send and receive with topic, one durable subscriber", "Topic10.jpg");
      
      chartVariableBenchmark("MessageSizeThroughput", "messageSize", "Message size (bytes)", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, one non-durable subscriber with varying message size, AUTO_ACKNOWLEDGE",
            "MessageSizeThroughput.jpg");
      
      chartVariableBenchmark("QueueScale1", "numberOfQueues", "Number of Queues", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with queue, varying number of queues, AUTO_ACKNOWLEDGE",
            "QueueScale1.jpg");
      
      chartVariableBenchmark("QueueScale2", "numberOfConnections", "Number of sending Connections", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with queue, varying number of sending connections, AUTO_ACKNOWLEDGE",
            "QueueScale2.jpg");
      
      chartVariableBenchmark("QueueScale3", "numberOfSessions", "Number of sending sessions", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with queue, varying number of sending sessions sharing same connection, AUTO_ACKNOWLEDGE",
            "QueueScale3.jpg");
      
      chartVariableBenchmark("TopicScale1", "numberOfTopics", "Number of Topics", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, varying number of topics, AUTO_ACKNOWLEDGE",
            "TopicScale1.jpg");
      
      chartVariableBenchmark("TopicScale2", "numberOfConnections", "Number of non-durable subscribers", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, varying number of subscribers each on own connection, AUTO_ACKNOWLEDGE",
            "TopicScale2.jpg");
      
      chartVariableBenchmark("TopicScale3", "numberOfSubscribers", "Number of non-durable subscribers", "Throughput (messages/sec)",
            "Non-transactional, non-persistent send and receive with topic, varying number of subscribers sharing same connection, AUTO_ACKNOWLEDGE",
            "TopicScale3.jpg");
      
      chartVariableBenchmark("MessageTypes", "messageType", "Message type (1=Message, 2=Bytes, 3=Map, 4=Object, 5=Stream, 6=Text, 7=Foreign", "Throughput (messages/sec)",
            "Send/receive messages to queue, vary by message type",
            "MessageTypes.jpg");
      
   }
   
   protected void chartVariableBenchmark(String bmName, String variableName, String xLabel, String yLabel, String title, String imageFileName)
      throws Exception
   {
      log.info("Charting benchmark:" + bmName);
      Benchmark bm = pm.getBenchmark(bmName);
      
      if (bm == null)
      {
         return;
      }
      
      XYSeriesCollection dataset = new XYSeriesCollection();
      
      //We chart each execution on a different graph - we could instead overlay them on the same graph
      Iterator executions = bm.getExecutions().iterator();
      int count = 0;
      while (executions.hasNext())
      {
         Execution exec = (Execution)executions.next();
         
         XYSeries series1 = new XYSeries(exec.getProvider() + " on " + exec.getDate());
         
         Iterator measurements = exec.getMeasurements().iterator();
         while (measurements.hasNext())
         {
            Measurement measurement = (Measurement)measurements.next();
            double variable = measurement.getVariableValue(variableName);
            
            series1.add(new Double(variable), measurement.getValue());
              
         }
                
         dataset.addSeries(series1);
                       
      }
      
      JFreeChart chart = ChartFactory.createXYLineChart(
            bmName + " - " + title,
            xLabel,
            yLabel,
            dataset,
            PlotOrientation.VERTICAL,
            true, 
            true, 
            false
            );
      
      createImage(chart, imageFileName);
      
   }
   
   protected void setUp() throws Exception
   {
      String dbURL = System.getProperty("perf.dbURL", "jdbc:hsqldb:hsql://localhost:7776");
      
      pm = new JDBCPersistenceManager(dbURL);
      pm.start();
      
      outputDir = new File(OUTPUT_DIR);
      
      File outputFile = new File(outputDir, OUTPUT_FILE);
      
      writer = new FileWriter(outputFile);
   }
   
   protected void tearDown() throws Exception
   {
      pm.stop();
      writer.close();
   }
   
   
   protected void run()
   {
      try
      {
         log.info("Starting perfcharter");
         
         setUp();
         
         writer.write("<html><body>\n");
         
         doCharts();
         
         writer.write("</body></html>\n");
         
         tearDown();
         
         log.info("Done");
      }
      catch (Exception e)
      {
         log.error("Failed to chart", e);
      }
   }
   

   protected void chartSimpleBenchmark(String bmName, String xLabel, String yLabel, String title, String imageFileName)
      throws Exception
   {
      log.info("Charting benchmark:" + bmName);
      
      Benchmark bm = pm.getBenchmark(bmName);
      
      if (bm == null)
      {
         return;
      }
      
      Map seriesMap = new HashMap();
      
      XYSeriesCollection dataset = new XYSeriesCollection();
      
      Iterator iter = bm.getExecutions().iterator();
      while (iter.hasNext())
      {
         Execution exec = (Execution)iter.next();
         Measurement measure = (Measurement)exec.getMeasurements().get(0);
         
         log.trace("Execution date is: " + exec.getDate());
         
         XYSeries xy = (XYSeries)seriesMap.get(exec.getProvider());
         if (xy == null)
         {
            xy = new XYSeries(exec.getProvider());
            dataset.addSeries(xy);
            seriesMap.put(exec.getProvider(), xy);
         }
         
         log.trace("Adding:" + exec.getDate().getTime() + ", value:" + measure.getValue());
         xy.add(exec.getDate().getTime(), measure.getValue());

         log.trace("Value is: " + measure.getValue());
      }      
      
      JFreeChart chart = ChartFactory.createTimeSeriesChart(
            title, 
            xLabel,
            yLabel,
            dataset, 
            true, 
            true,
            false 
            );      

      createImage(chart, imageFileName);
      
   }
   
   
   protected void createImage(JFreeChart chart, String imageFileName) throws Exception
   {
      XYPlot plot = (XYPlot)chart.getPlot();
      
      chart.setBackgroundPaint(Color.white);
      
      plot.setBackgroundPaint(Color.lightGray);
      plot.setRangeGridlinePaint(Color.white);
      
      ValueAxis rangeAxis = plot.getRangeAxis();
      
      NumberAxis axis = (NumberAxis)rangeAxis;
      axis.setAutoRangeIncludesZero(true);
      
      XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
      renderer.setShapesVisible(true);
      renderer.setDrawOutlines(true);
      renderer.setUseFillPaint(true);
       
      File imageFile = new File(outputDir, imageFileName);
      
      FileOutputStream fos = new FileOutputStream(imageFile);
      
      JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(fos);
      BufferedImage image = chart.createBufferedImage(1000, 400);
      
      encoder.encode(image);
      
      fos.close();
      
      writer.write("<img src=\"" + imageFileName + "\"><br>\n");  
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
 
}
