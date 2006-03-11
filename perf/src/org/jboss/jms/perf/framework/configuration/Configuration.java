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
import org.jboss.jms.perf.framework.protocol.DrainJob;
import org.jboss.jms.perf.framework.Runner;
import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.protocol.SendJob;
import org.jboss.jms.perf.framework.protocol.ReceiveJob;
import org.jboss.jms.perf.framework.protocol.Job;
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
   private List performanceTests;

   // Map<providerName - Properties>
   private Map jndiProperties;

   private JobConfiguration defaultsPerBenchmark;

   // Constructors --------------------------------------------------

   public Configuration(Runner runner, File xmlConfiguration) throws Exception
   {
      this.runner = runner;
      this.xmlConfiguration = xmlConfiguration;

      performanceTests = new ArrayList();
      jndiProperties = new HashMap();

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

   public Properties getJNDIProperties(String providerName)
   {
      Properties p = (Properties)jndiProperties.get(providerName);
      if (p == null)
      {
         throw new IllegalStateException("No such provider: " + providerName);
      }
      return p;
   }

   public List getPerformanceTests()
   {
      return performanceTests;
   }

   /**
    * @return a List of Strings
    */
   public List getExecutorURLs()
   {
      List result = new ArrayList();
      for(Iterator i = performanceTests.iterator(); i.hasNext(); )
      {
         PerformanceTest pt = (PerformanceTest)i.next();
         for(Iterator ji = pt.iterator(); ji.hasNext(); )
         {
            Object o = ji.next();

            if (o instanceof Job)
            {
               String executorURL = ((Job)o).getExecutorURL();
               if (!result.contains(executorURL))
               {
                  result.add(executorURL);
               }
            }
            else
            {
               JobList jl = (JobList)o;
               for(Iterator jli = jl.iterator(); jli.hasNext(); )
               {
                  String executorURL = ((Job)jli.next()).getExecutorURL();
                  if (!result.contains(executorURL))
                  {
                     result.add(executorURL);
                  }
               }
            }
         }
      }
      return result;
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
               else if ("providers".equals(name))
               {
                  extractProviders(n);
               }
               else if (JobConfiguration.isValidElementName(name))
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
         }
         jndiProperties.put(providerName, props);
      }
   }

   private void extractPerformanceTests(Node tests)
      throws Exception
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

      PerformanceTest pt = new PerformanceTest(runner, performanceTestName);

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

            if (JobConfiguration.isValidElementName(name))
            {
               defaultsPerTest.add(n);
            }
            else if ("drain".equals(name))
            {
               addDrainJob(pt, n, defaultsPerTest);
            }
            else if ("send".equals(name))
            {
               addSenderJob(pt, n, defaultsPerTest);
            }
            else if ("receive".equals(name))
            {
               addReceiverJob(pt, n, defaultsPerTest);
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

      if (pt.getExecutions().isEmpty())
      {
         throw new Exception("Performance test \"" + performanceTestName + "\" has no executions!");
      }

      performanceTests.add(pt);
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

      pt.addExecution(new Execution(providerName));
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
         String name = n.getNodeName();
         if ("send".equals(name))
         {
            addSenderJob(parallelJobs, n, defaultPerTest);
         }
         else if ("receive".equals(name))
         {
            addReceiverJob(parallelJobs, n, defaultPerTest);
         }
      }

      pt.addParallelJobs(parallelJobs);
   }

   private void addDrainJob(JobList jl, Node drain, JobConfiguration defaultPerTest)
      throws Exception
   {
      DrainJob dj = new DrainJob();
      setCommonJobAttributes(dj, drain, defaultPerTest);
      jl.addJob(dj);
   }

   private void addSenderJob(JobList jl, Node send, JobConfiguration defaultPerTest)
      throws Exception
   {
      SendJob sj = new SendJob();
      setCommonJobAttributes(sj, send, defaultPerTest);
      jl.addJob(sj);
   }

   private void addReceiverJob(JobList jl, Node receive, JobConfiguration defaultPerTest)
      throws Exception
   {
      ReceiveJob rj = new ReceiveJob();
      setCommonJobAttributes(rj, receive, defaultPerTest);
      jl.addJob(rj);
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
            if (!JobConfiguration.isValidElementName(name))
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
            if (!JobConfiguration.isValidElementName(name))
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
