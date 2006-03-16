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

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.data.GraphInfo;
import org.jboss.jms.perf.framework.data.AxisInfo;
import org.jboss.jms.perf.framework.persistence.PersistenceManager;
import org.jboss.jms.perf.framework.protocol.Failure;
import org.jboss.jms.perf.framework.protocol.ThroughputResult;
import org.jboss.jms.perf.framework.protocol.Job;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.configuration.Configuration;
import org.jboss.logging.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.renderer.AbstractRenderer;
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

   public static final DateFormat dateFormat = new SimpleDateFormat("MMM d yy HH:mm a");

   // Static --------------------------------------------------------

   String generateImageName(String testName)
   {
      StringBuffer sb = new StringBuffer();
      for(StringTokenizer st = new StringTokenizer(testName, " \t,;!:-"); st.hasMoreTokens();)
      {
         String s = st.nextToken();
         sb.append(s);
      }
      sb.append(".jpg");
      return sb.toString();
   }

   // Attributes ----------------------------------------------------

   protected PersistenceManager pm;
   protected Configuration configuration;
   protected Writer writer;
   protected File outputDir;

   // Constructors --------------------------------------------------

   /**
    * It is not the Charter's responsibility to start/stop the peristence manager the database.
    */
   Charter(PersistenceManager pm, Configuration configuration)
   {
      this.pm = pm;
      this.configuration = configuration;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void run() throws Exception
   {
      outputDir = new File(configuration.getReportDirectory());
      writer = new FileWriter(new File(outputDir, OUTPUT_FILE));

      try
      {
         writer.write("<html><body>\n");
         doCharts();
         writer.write("</body></html>\n");
         log.debug("success");
      }
      finally
      {
         writer.close();
      }
   }

   protected void doCharts() throws Exception
   {
      // one chart (image) per performance test

      // only chart the performance tests from the current configuration file; the database may
      // contain much more that those, but it MUST contains the ones from the config file, they
      // just ran

      for(Iterator i = configuration.getPerformanceTests().iterator(); i.hasNext(); )
      {
         String testName = ((PerformanceTest)i.next()).getName();
         PerformanceTest pt = pm.getPerformanceTest(testName);

         if (pt == null)
         {
            throw new Exception("The performance test '" + testName + "' found in configuration " +
                                "file but not in the database");
         }
         chartPerformanceTest(pt);
      }
   }

   protected void chartPerformanceTest(PerformanceTest pt) throws Exception
   {
      String testName = pt.getName();

      GraphInfo graphInfo = null;

      // we only have graph info in memory, not in database
      for(Iterator i = configuration.getPerformanceTests().iterator(); i.hasNext(); )
      {
         PerformanceTest thispt = (PerformanceTest)i.next();
         if (thispt.getName().equals(testName))
         {
            graphInfo = thispt.getGraphInfo();
            break;
         }
      }

      if (graphInfo == null)
      {
         log.warn("No <graph> section for " + testName + ", skipping generating chart");
         return;
      }
      

      XYSeriesCollection dataset = new XYSeriesCollection();
      ProviderToSeriesIndexMapper providerToSeries = new ProviderToSeriesIndexMapper();

      for(Iterator i = pt.getEffectiveExecutions().iterator(); i.hasNext(); )
      {
         Execution e = (Execution)i.next();
         chartExecution(dataset, e, providerToSeries, graphInfo);
      }

      String xLabel = graphInfo.getAxisInfo(GraphInfo.X).getLabel();
      xLabel = xLabel == null ? "undefined label" : xLabel;
      String yLabel = graphInfo.getAxisInfo(GraphInfo.Y).getLabel();
      yLabel = yLabel == null ? "undefined label" : yLabel;

      JFreeChart chart =
         ChartFactory.createXYLineChart(testName, xLabel, yLabel, dataset, PlotOrientation.VERTICAL,
                                        true, true, false);

      createImage(chart, providerToSeries, generateImageName(testName));
   }

   protected void chartExecution(XYSeriesCollection dataset, Execution execution,
                                 ProviderToSeriesIndexMapper providerToSeries,
                                 GraphInfo graphInfo) throws Exception
   {
      log.info("Charting " + execution);

      String providerName = execution.getProviderName();

      AxisInfo xAxis = graphInfo.getAxisInfo(GraphInfo.X);
      AxisInfo yAxis = graphInfo.getAxisInfo(GraphInfo.Y);

      String seriesDescription =
         generateSeriesDescription(dataset, providerName, execution.getStartDate());

      XYSeries series = new XYSeries(seriesDescription);

      outer:for(Iterator i = execution.iterator(); i.hasNext(); )
      {
         List measurement = (List)i.next();

         double x = 0, y = 0;

         // TODO this won't work for multiple identical jobs, and also for more than one
         //      parallel jobs
         for(Iterator j = measurement.iterator(); j.hasNext(); )
         {
            Result result = (Result)j.next();

            if (result instanceof Failure)
            {
               // ignore this measurment
               continue outer;
            }

            Job job = (Job)result.getRequest();

            boolean addToGraph = false;

            if (job.getType() == xAxis.getJobType())
            {
               if (xAxis.isResult())
               {
                  // TODO basically we're ignoring that we can also want the number of messages
                  x = ((ThroughputResult)result).getThroughput();
                  log.debug("recording " + x + " on the x axis, as result");
               }
               else
               {
                  // TODO basically we're ignoring anything else but rates
                  x = job.getRate();
                  log.debug("recording " + x + " on the x axis");
               }

               addToGraph = true;
            }

            if (job.getType() == yAxis.getJobType())
            {
               if (yAxis.isResult())
               {
                  // TODO basically we're ignoring that we can also want the number of messages
                  y = ((ThroughputResult)result).getThroughput();
                  log.debug("recording " + y + " on the y axis, as result");
               }
               else
               {
                  // TODO basically we're ignoring anything else but rates
                  y = job.getRate();
                  log.debug("recording " + y + " on the y axis");
               }

               addToGraph = true;
            }

            if (addToGraph)
            {
               series.add(x, y);
            }
         }
      }

      dataset.addSeries(series);
      providerToSeries.newSeries(providerName);
   }


   // Private -------------------------------------------------------

   private String generateSeriesDescription(XYSeriesCollection dataset,
                                            String providerName,
                                            Date executionStartDate)
   {
      String seriesDescriptionBase =
         providerName + " (" +
            (executionStartDate == null ? "Not Dated" : dateFormat.format(executionStartDate));

      // make sure I don't have already a series with the same description; this is possible if more
      // than one execution start the same minute

      int counter = 0;
      String seriesDescription = null;

      outer:while(true)
      {
         seriesDescription =
            seriesDescriptionBase + (counter == 0 ? "" : " " + Integer.toString(counter)) + ")";

         for(Iterator i = dataset.getSeries().iterator(); i.hasNext(); )
         {
            XYSeries s = (XYSeries)i.next();
            String sd = s.getDescription();
            // TODO: apparently there is a bug in JFreeCharts so s.getDescription() would return
            //       null here, so this mechanism of nicely indexing series generated in the same
            //       minute doesn't work. No big deal, though.
            if (seriesDescription.equals(sd))
            {
               counter ++;
               continue outer;
            }
         }

         break;
      }

      return seriesDescription;
   }

   private void createImage(JFreeChart chart, ProviderToSeriesIndexMapper providerToSeries,
                            String imageFileName) throws Exception
   {
      XYPlot plot = (XYPlot)chart.getPlot();

      chart.setBackgroundPaint(Color.white);

      plot.setBackgroundPaint(Color.lightGray);
      plot.setRangeGridlinePaint(Color.white);

      ValueAxis rangeAxis = plot.getRangeAxis();

      NumberAxis axis = (NumberAxis)rangeAxis;

      axis.setAutoRangeIncludesZero(true);

      XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer)plot.getRenderer();

      renderer.setShapesVisible(true);
      renderer.setDrawOutlines(true);
      renderer.setUseFillPaint(true);

      adjustColors(renderer, providerToSeries);

      File imageFile = new File(outputDir, imageFileName);

      FileOutputStream fos = new FileOutputStream(imageFile);

      JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(fos);
      BufferedImage image = chart.createBufferedImage(1000, 1000);

      encoder.encode(image);

      fos.close();

      writer.write("<img src=\"" + imageFileName + "\"><br>\n");
   }




   /**
    * Make series corresponding to the same provider have close colors.
    */
   private void adjustColors(AbstractRenderer renderer,
                             ProviderToSeriesIndexMapper providerToSeries)
   {

      int colorStep = configuration.getColorStep();

      for(Iterator i = providerToSeries.providerNames().iterator(); i.hasNext(); )
      {
         String providerName = (String)i.next();

         Color baseColor = configuration.getProvider(providerName).getColor();
         int baser = baseColor.getRed();
         int baseg = baseColor.getGreen();
         int baseb = baseColor.getBlue();

         List indexes = (List)providerToSeries.getIndexes(providerName);

         // uniformly spread the colors around the base color
         int offset = 0;
         for(Iterator j = indexes.iterator(); j.hasNext(); offset++)
         {
            int index = ((Integer)j.next()).intValue();

            int newr = limit(baser + offset * colorStep);
            int newg = limit(baseg + offset * colorStep);
            int newb = limit(baseb + offset * colorStep);

            renderer.setSeriesPaint(index, new Color(newr, newg, newb));
         }

      }
   }

   private int limit(int color)
   {
      if (color < 0)
      {
         return 0;
      }
      else if (color > 255)
      {
         return 255;
      }
      else
      {
         return color;
      }
   }

   // Inner classes -------------------------------------------------

   private class ProviderToSeriesIndexMapper
   {
      private int index;
      private Map providerToIndexes;

      private ProviderToSeriesIndexMapper()
      {
         index = 0;
         providerToIndexes = new HashMap();
      }

      private void newSeries(String providerName)
      {
         List indexes = (List)providerToIndexes.get(providerName);
         if (indexes == null)
         {
            indexes = new ArrayList();
            providerToIndexes.put(providerName, indexes);
         }
         indexes.add(new Integer(index++));
      }

      private Set providerNames()
      {
         return providerToIndexes.keySet();
      }

      private List getIndexes(String providerName)
      {
         return (List)providerToIndexes.get(providerName);
      }
   }

}
