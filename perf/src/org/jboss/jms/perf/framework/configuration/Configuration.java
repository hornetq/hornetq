/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.configuration;

import org.jboss.jms.util.XMLUtil;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.JobList;
import org.jboss.jms.perf.framework.data.SimpleJobList;
import org.jboss.jms.perf.framework.data.Provider;
import org.jboss.jms.perf.framework.data.GraphInfo;
import org.jboss.jms.perf.framework.data.AxisInfo;
import org.jboss.jms.perf.framework.protocol.DrainJob;
import org.jboss.jms.perf.framework.Runner;
import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.protocol.SendJob;
import org.jboss.jms.perf.framework.protocol.ReceiveJob;
import org.jboss.jms.perf.framework.protocol.Job;
import org.jboss.jms.perf.framework.protocol.PingJob;
import org.jboss.jms.perf.framework.protocol.JobSupport;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.awt.*;

/**
 * A performance run configuration.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class Configuration
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static boolean toBoolean(String os)
   {
      if (os == null)
      {
         throw new IllegalArgumentException("literal boolean required");
      }

      String s = os.toLowerCase();

      if ("true".equals(s) || "yes".equals(s))
      {
         return true;
      }
      else if ("false".equals(s) || "no".equals(s))
      {
         return false;
      }

      throw new IllegalArgumentException("invalid boolean literal: " + os);
   }

   public static String coordinatorTypeToString(int type)
   {
      if (Coordinator.JBOSSREMOTING == type)
      {
         return "JBoss Remoting";
      }
      else if (Coordinator.RMI == type)
      {
         return "RMI";
      }
      else
      {
         return "UNKNOWN (" + type +")";
      }
   }

   // Attributes ----------------------------------------------------

   private File xmlConfiguration;
   private Runner runner;

   private String dbURL;
   private String reportDirectory;
   private boolean startExecutors;
   private String defaultExecutorURL;
   int colorStep;
   private List performanceTests;

   // Map<providerName - provider>
   private Map providers;

   private JobConfiguration defaultsPerBenchmark;

   // Constructors --------------------------------------------------

   public Configuration(Runner runner, File xmlConfiguration) throws Exception
   {
      this.runner = runner;
      this.xmlConfiguration = xmlConfiguration;

      performanceTests = new ArrayList();
      providers = new HashMap();

      defaultsPerBenchmark = new JobConfiguration();

      parse();
      validate();
   }

   // Public --------------------------------------------------------

   public String getDatabaseURL()
   {
      return dbURL;
   }

   public String getReportDirectory()
   {
      return reportDirectory;
   }

   public boolean isStartExecutors()
   {
      return startExecutors;
   }

   public String getDefaultExecutorURL()
   {
      return defaultExecutorURL;
   }

   public int getColorStep()
   {
      return colorStep;
   }

   public List getPerformanceTests()
   {
      return performanceTests;
   }

   public Provider getProvider(String providerName)
   {
      return (Provider)providers.get(providerName);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append("Configuration:\n");
      sb.append("    dbURL: ").append(dbURL).append('\n');
      sb.append("Tests:\n");
      for(Iterator i = performanceTests.iterator(); i.hasNext(); )
      {
         PerformanceTest pt = (PerformanceTest)i.next();
         sb.append("    ").append(pt).append('\n');
      }
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void parse() throws Exception
   {

      Reader reader = new FileReader(xmlConfiguration);

      try
      {
         Element root = XMLUtil.readerToElement(reader);

         if (!"benchmark".equals(root.getNodeName()))
         {
            throw new Exception("Invalid root element: " + root.getNodeName());
         }

         if (root.hasChildNodes())
         {
            NodeList nl = root.getChildNodes();
            for(int i = 0; i < nl.getLength(); i++)
            {
               Node n = nl.item(i);
               String name = n.getNodeName();

               if ("db-url".equals(name))
               {
                  dbURL = XMLUtil.getTextContent(n);
               }
               else if ("report-directory".equals(name))
               {
                  reportDirectory = XMLUtil.getTextContent(n);
               }
               else if ("start-executors".equals(name))
               {
                  startExecutors = toBoolean(XMLUtil.getTextContent(n));
               }
               else if ("default-executor-url".equals(name))
               {
                  defaultExecutorURL = XMLUtil.getTextContent(n);
               }
               else if ("color-step".equals(name))
               {
                  colorStep = Integer.parseInt(XMLUtil.getTextContent(n));
               }
               else if ("providers".equals(name))
               {
                  extractProviders(n);
               }
               else if (JobConfiguration.isValidJobConfigurationElementName(name))
               {
                  defaultsPerBenchmark.add(n);
               }
               else if ("performance-tests".equals(name))
               {
                  extractPerformanceTests(n);
               }
               else
               {
                  if (!name.startsWith("#"))
                  {
                     throw new Exception("Unexpected child <" + name +
                                         "> of node " + root.getNodeName());
                  }
               }
            }
         }
      }
      finally
      {
         reader.close();
      }
   }

   private void extractProviders(Node providers) throws Exception
   {
      if (!providers.hasChildNodes())
      {
         return;
      }

      NodeList nl = providers.getChildNodes();
      for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         String name = n.getNodeName();
         if ("provider".equals(name))
         {
            addProvider(n);
         }
      }
   }

   private void addProvider(Node pn) throws Exception
   {
      NamedNodeMap attrs = pn.getAttributes();
      Node nameNode = attrs.getNamedItem("name");
      String providerName = nameNode.getNodeValue();

      Provider provider = new Provider(providerName);

      if (pn.hasChildNodes())
      {
         Properties props = new Properties();
         NodeList nl = pn.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);
            String name = n.getNodeName();
            String value = XMLUtil.getTextContent(n);
            if ("factory".equals(name))
            {
               props.setProperty("java.naming.factory.initial", value);
            }
            else if ("url".equals(name))
            {
               props.setProperty("java.naming.provider.url", value);
            }
            else if ("pkg".equals(name))
            {
               props.setProperty("java.naming.factory.url.pkg", value);
            }
            else if ("executor".equals(name))
            {
               addExecutor(provider, n);
            }
            else if ("color".equals(name))
            {
               addColor(provider, n);
            }
            else if (name.startsWith("#"))
            {
               // ignore
            }
            else
            {
               throw new Exception("Unknown provider configuration element: " + name);
            }
         }

         provider.setJNDIProperties(props);
         providers.put(providerName, provider);
      }
   }

   private void addExecutor(Provider provider, Node n) throws Exception
   {
      NamedNodeMap attrs = n.getAttributes();
      Node name = attrs.getNamedItem("name");
      Node url = attrs.getNamedItem("url");
      provider.addExecutor(name.getNodeValue(), url.getNodeValue());
   }

   private void addColor(Provider provider, Node n) throws Exception
   {
      String color = XMLUtil.getTextContent(n);
      StringTokenizer st = new StringTokenizer(color, ",; ");

      String reds = st.nextToken();
      int red = Integer.parseInt(reds);
      String greens = st.nextToken();
      int green = Integer.parseInt(greens);
      String blues = st.nextToken();
      int blue = Integer.parseInt(blues);

      provider.setColor(new Color(red, green, blue));
   }

   private void extractPerformanceTests(Node tests) throws Exception
   {
      if (!tests.hasChildNodes())
      {
         return;
      }

      NodeList nl = tests.getChildNodes();
      for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         String name = n.getNodeName();
         if ("performance-test".equals(name))
         {
            addPerformanceTest(n);
         }
         else
         {
            if(!name.startsWith("#"))
            {
               throw new Exception("Unexpected child <" + name +
                                   "> of node " + tests.getNodeName());
            }
         }
      }
   }

   private void addPerformanceTest(Node test) throws Exception
   {
      NamedNodeMap attrs = test.getAttributes();

      Node nameNode = attrs.getNamedItem("name");
      String performanceTestName = nameNode.getNodeValue();

      Node loopsNode = attrs.getNamedItem("loops");
      int loops = 1;
      if (loopsNode != null)
      {
         loops = Integer.parseInt(loopsNode.getNodeValue());
      }

      PerformanceTest pt = new PerformanceTest(runner, performanceTestName, loops);

      for(Iterator i = performanceTests.iterator(); i.hasNext(); )
      {
         PerformanceTest t = (PerformanceTest)i.next();
         if (t.getName().equals(pt.getName()))
         {
            throw new Exception("Duplicate performance test name: " + performanceTestName);
         }
      }

      if (test.hasChildNodes())
      {
         NodeList nl = test.getChildNodes();
         JobConfiguration defaultsPerTest = defaultsPerBenchmark.copy();

         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);
            String name = n.getNodeName();

            if ("graph".equals(name))
            {
               addGraphDetails(pt, n);
            }
            else if (JobConfiguration.isValidJobConfigurationElementName(name))
            {
               defaultsPerTest.add(n);
            }
            else if (JobSupport.isValidJobType(name))
            {
               addJob(pt, n, defaultsPerTest);
            }
            else if ("parallel".equals(name))
            {
               addParallel(pt, n, defaultsPerTest);
            }
            else if ("execution".equals(name))
            {
               addExecution(pt, n);
            }
            else
            {
               if (!name.startsWith("#"))
               {
                  throw new Exception("Unexpected child <" + name +
                     "> of node " + test.getNodeName());
               }
            }
         }
      }

      performanceTests.add(pt);
   }

   private void addGraphDetails(PerformanceTest pt, Node graph) throws Exception
   {
      if (!graph.hasChildNodes())
      {
         throw new Exception("The graph node has no children");
      }

      GraphInfo graphInfo = new GraphInfo();
      pt.setGraphInfo(graphInfo);
      
      NodeList nl = graph.getChildNodes();
      for(int i = 0; i < nl.getLength(); i++)
      {

         Node n = nl.item(i);
         String name = n.getNodeName();
         if ("x".equals(name))
         {
            addAxisInfo(graphInfo, GraphInfo.X, n);
         }
         else if ("y".equals(name))
         {
            addAxisInfo(graphInfo, GraphInfo.Y, n);
         }
         else
         {
            if (!name.startsWith("#"))
            {
               throw new Exception("Unknow graph axis: " + name);
            }
         }
      }
   }

   private void addAxisInfo(GraphInfo graphInfo, int axisType, Node axis) throws Exception
   {
      if (!axis.hasAttributes())
      {
         throw new Exception("Axis " + GraphInfo.axisTypeToString(axisType) + " info incomplete");
      }

      AxisInfo info = new AxisInfo();

      NamedNodeMap attrs = axis.getAttributes();
      for(int i = 0; i < attrs.getLength(); i++)
      {
         Node n = attrs.item(i);
         String name = n.getNodeName();
         String value = n.getNodeValue();
         if ("job".equals(name))
         {
            info.setJobType(JobSupport.getJobType(value));
         }
         else if ("metric".equals(name))
         {
            // TODO - this is kind of flaky, but it'll work for now
            info.setMetric(value);
         }
         else if ("label".equals(name))
         {
            info.setLabel(value);
         }
         else if ("result".equals(name))
         {
            info.setResult(toBoolean(value));
         }
         else
         {
            if (!name.startsWith("#"))
            {
               throw new Exception("Unknow graph axis: " + name);
            }
         }
      }

      if (info.getJobType() == null)
      {
         throw new Exception("Axis info incompletely specified, job type missing");
      }

      if (info.getMetric() == null)
      {
         throw new Exception("Axis info incompletely specified, metric missing");
      }

      graphInfo.addAxisInfo(axisType, info);
   }

   private void addExecution(PerformanceTest pt, Node execution) throws Exception
   {
      String providerName = null;

      if (execution.hasChildNodes())
      {
         NodeList nl = execution.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);
            String name = n.getNodeName();
            if ("provider".equals(name))
            {
               providerName = XMLUtil.getTextContent(n);
            }
         }
      }
      else
      {
         if (execution.hasAttributes())
         {
            NamedNodeMap attrs = execution.getAttributes();
            Node n = attrs.getNamedItem("provider");
            providerName = n.getNodeValue();
         }
      }

      if (providerName == null)
      {
         throw new Exception("No provider name found!");
      }

      pt.addRequestedExecution(new Execution(providerName));
   }

   private void addParallel(PerformanceTest pt, Node pn, JobConfiguration defaultPerTest)
      throws Exception
   {
      if (!pn.hasChildNodes())
      {
         return;
      }

      JobList parallelJobs = new SimpleJobList();

      NodeList nl = pn.getChildNodes();
      for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         addJob(parallelJobs, n, defaultPerTest);
      }
      pt.addParallelJobs(parallelJobs);
   }

   private void addJob(JobList jl, Node n, JobConfiguration defaultPerTest) throws Exception
   {
      String name = n.getNodeName();
      Job job = null;

      if (name.startsWith("#"))
      {
         return;
      }
      else if ("drain".equals(name))
      {
         job = new DrainJob();
      }
      else if ("send".equals(name))
      {
         job = new SendJob();
      }
      else if ("receive".equals(name))
      {
         job = new ReceiveJob();
      }
      else if ("ping".equals(name))
      {
         job = new PingJob();
      }
      else
      {
         throw new Exception("Unknown job " + name);
      }
      setCommonJobAttributes(job, n, defaultPerTest);
      jl.addJob(job);
   }

   private void setCommonJobAttributes(Job j, Node jn, JobConfiguration defaultPerTest)
      throws Exception
   {
      JobConfiguration config = defaultPerTest.copy();

      // job configuration attributes can be specified as XML attributes or sub-elements.
      // Sub-elements have priority
      if (jn.hasAttributes())
      {
         NamedNodeMap attrs = jn.getAttributes();
         for(int i = 0; i < attrs.getLength(); i++)
         {
            Node n = attrs.item(i);
            String name = n.getNodeName();
            if (!JobConfiguration.isValidJobConfigurationElementName(name))
            {
               throw new Exception("Invalid job configuration attribute: " + name);
            }
            config.add(n);
         }
      }

      if (jn.hasChildNodes())
      {
         NodeList nl = jn.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);
            String name = n.getNodeName();
            if (name.startsWith("#"))
            {
               continue;
            }
            if (!JobConfiguration.isValidJobConfigurationElementName(name))
            {
               throw new Exception("Invalid job configuration element: " + name);
            }
            config.add(n);
         }
      }
      config.configure(j);
   }

   private void validate() throws Exception
   {
      if (dbURL == null)
      {
         throw new Exception("No <db-url> element in " + xmlConfiguration);
      }
   }

   // Inner classes -------------------------------------------------
}
