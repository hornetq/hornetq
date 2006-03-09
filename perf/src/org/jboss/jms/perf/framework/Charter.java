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
import java.util.List;

import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.PerformanceTest;
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
 * Chars performance test results
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
class Charter
{
   // Constants -----------------------------------------------------

   //TODO configure from ant task
   private static final String OUTPUT_FILE = "benchmark-results.html";
   private static final Logger log = Logger.getLogger(Charter.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String outputDirName;
   protected PersistenceManager pm;
   protected File outputDir;
   protected Writer writer;

   // Constructors --------------------------------------------------

   /**
    * It is not the Charter's responsibility to start/stop the peristence manager the database.
    * @param pm
    * @param outputDirName
    */
   Charter(PersistenceManager pm, String outputDirName)
   {
      this.pm = pm;
      this.outputDirName = outputDirName;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void run() throws Exception
   {
      outputDir = new File(outputDirName);
      writer = new FileWriter(new File(outputDir, OUTPUT_FILE));

      try
      {
         writer.write("<html><body>\n");
         doCharts();
         writer.write("</body></html>\n");
      }
      finally
      {
         writer.close();
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

   protected void doCharts() throws Exception
   {

      // one chart (image) per performance test

      List ptests = pm.getPerformanceTestNames();
      for(Iterator i = ptests.iterator(); i.hasNext(); )
      {
         String perfTestName = (String)i.next();
         PerformanceTest pt = pm.getPerformanceTest(perfTestName);
         chartPerformanceTest(pt);
      }
   }

   protected void chartPerformanceTest(PerformanceTest pt) throws Exception
   {
      // a chart depicts more executions

      for(Iterator i = pt.getExecutions().iterator(); i.hasNext(); )
      {
         Execution e = (Execution)i.next();
         chartExecution(pt.getName(), e);
      }
   }

   protected void chartExecution(String testName, Execution execution) throws Exception
   {
      String providerName = execution.getProviderName();

      XYSeries series = new XYSeries(providerName);
      for(Iterator i = execution.iterator(); i.hasNext(); )
      {
         List measurement = (List)i.next();

         // TODO This is a particular case, make it more general

         if (measurement.size() != 2)
         {
            // ignore datapoints that do not have 2 parallel measurements (e.g. drains)
            continue;
         }

         ThroughputResult sendRate = (ThroughputResult)measurement.get(0);
         ThroughputResult receiveRate = (ThroughputResult)measurement.get(1);

         if (sendRate.getJob().getType() == ReceiverJob.TYPE)
         {
            ThroughputResult tmp = sendRate;
            sendRate = receiveRate;
            receiveRate = tmp;
         }

         series.add(sendRate.getThroughput(), receiveRate.getThroughput());
      }

      XYSeriesCollection dataset = new XYSeriesCollection();
      dataset.addSeries(series);

      JFreeChart chart =
         ChartFactory.createXYLineChart(testName, "send rate (msg/s)", "receive rate (msg/s)",
                                        dataset, PlotOrientation.VERTICAL, true, true, false);

      createImage(chart, "image.jpg");
   }




   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
