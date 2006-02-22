/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.jms.perf.framework;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Iterator;

import org.jboss.jms.perf.framework.data.Datapoint;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.Measurement;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.persistence.HSQLDBPersistenceManager;
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

/**
 * 
 * A Charter.
 * 
 * Chars performance test results
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Charter
{
   //TODO configure from ant task
   private static final String OUTPUT_DIR = "./output";
   
   private static final String OUTPUT_FILE = "jms-perf-results.html";
   
   private static final String DB_URL = "jdbc:hsqldb:./perfResultsDB";
   
   private static final Logger log = Logger.getLogger(Charter.class); 
   
   protected PersistenceManager pm;
   
   protected File outputDir;
   
   protected Writer writer;
   
   public static void main(String[] args)
   {
      new Charter().run();
   }
   
   
   protected void setUp() throws Exception
   {
      log.info("In setup");
      
      pm = new HSQLDBPersistenceManager(DB_URL);
      
      pm.start();
      
      log.info("Started db");
      
      outputDir = new File(OUTPUT_DIR);
      
      File outputFile = new File(outputDir, OUTPUT_FILE);
      
      log.info("Output file is " + outputFile);
      
      writer = new FileWriter(outputFile);
      
      log.info("finished setup");
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
         log.info("Starting perfcharter, ok");
         
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
         e.printStackTrace();
      }
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
      BufferedImage image = chart.createBufferedImage(1000, 1000);
      
      encoder.encode(image);
      
      fos.close();
      
      writer.write("<img src=\"" + imageFileName + "\"><br>\n");  
   }
   
   protected void chartExecution(Execution execution, String xLabel, String yLabel,
                                 String xDimension, String yDimension, String title,
                                 String imageFileName) throws Exception
   {
      log.info("Charting execution");
      
      XYSeriesCollection dataset = new XYSeriesCollection();
      
      XYSeries series = new XYSeries(execution.getProviderName() + " on " + execution.getDate());
      
      dataset.addSeries(series);
      
      Iterator iter = execution.getDatapoints().iterator();
      
      log.info("Datapoits:" + execution.getDatapoints().size());
      
      while (iter.hasNext())
      {
         Datapoint dp = (Datapoint)iter.next();
         
         Measurement xMeasurement = dp.getMeasurement(xDimension);
         
         Measurement yMeasurement = dp.getMeasurement(yDimension);
         
         log.info("x:" + xMeasurement.getValue() + " y:" + yMeasurement.getValue());
         
         series.add((Double)xMeasurement.getValue(), (Double)yMeasurement.getValue());         
      }
      
      JFreeChart chart = ChartFactory.createXYLineChart(
            title,
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
   
   protected void doCharts() throws Exception
   {
      chartQueue1();
   }
   
   protected void chartQueue1() throws Exception
   {
      PerformanceTest pt = pm.getPerformanceTest("Queue1");
      
      log.info("pt is:" + pt);
      
      log.info("exceutions:" + pt.getExecutions());
      
      log.info("exceutions size:" + pt.getExecutions().size());
      
      Iterator iter = pt.getExecutions().iterator();
      
      while (iter.hasNext())
      {
         Execution exec = (Execution)iter.next();
         
         chartExecution(exec, "Receive rate (msgs/s)", "Send rate (msgs/s)",
               "sendRate", "receiveRate", "Non transacted, non-persistent, 0K message",
               "Queue1" + exec.getDate().getTime() + ".jpg");
      }
   }
}
