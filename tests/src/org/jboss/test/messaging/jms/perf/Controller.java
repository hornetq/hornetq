/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Controller
{
   private static final Logger log = Logger.getLogger(Controller.class);   
   
   protected Map configs = new HashMap();
   
   protected List tests = new ArrayList();
   
   protected ResultPersistor persistor;
   
   public static void main(String[] args)
   {
      log.info("Controller starting");
      
      new Controller().run();
   }
   
   private Controller()
   {      
   }
   
   protected Object sendRequestToSlave(String slaveURL, ServerRequest request) throws Throwable
   {
      InvokerLocator locator = new InvokerLocator(slaveURL);
      Client client = new Client(locator, "perftest");
      Object res = client.invoke(request);
      return res;
   }
   
   protected Document getConfigDocument(String filename) throws Exception
   {
      InputStream is = null;
      try
      {
         File f = new File("controller.xml");   
         
         is = new FileInputStream(f);
         
         DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
   
         docBuilderFactory.setValidating(false);
   
         docBuilderFactory.setNamespaceAware(true);
   
         DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
         
         Document doc = docBuilder.parse(is);
         
         return doc;
      }
      finally
      {
         if (is != null)
         {
            try
            {
               is.close();
            }
            catch (IOException e)
            {
               log.error("Failed to close input stream", e);
            }
         }
      }
   }
   
   protected void init() throws Exception
   {
      Document doc = getConfigDocument("controller.xml");
      
      Element root = doc.getDocumentElement();
                
      log.info("root element name is " + root.getNodeName());
      
      
      Iterator iter = MetadataUtils.getChildrenByTagName(root, "config");
      
      while (iter.hasNext())
      {
         Element element = (Element)iter.next();
         Configuration config = new Configuration(element);   
         configs.put(config.getName(), config);
      }
      
      iter = MetadataUtils.getChildrenByTagName(root, "test");
      
      while (iter.hasNext())
      {
         Element element = (Element)iter.next();
         Test t = new Test(element);
         tests.add(t);
      }
      
      persistor = new CSVResultPersistor("perf-results.csv");
      
   }
   
   protected void startJobsAtSlave(SlaveMetadata slave) throws Throwable
   {
      Iterator iter = slave.getJobs().iterator();
      
      while (iter.hasNext())
      {
         Job job = (Job)iter.next();
         RunRequest req = new RunRequest(job);
         sendRequestToSlave(slave.getSlaveLocator(), req);
      }
      
   }
   
   protected void stopJobsAtSlave(SlaveMetadata slave) throws Throwable
   {
      StopRequest req = new StopRequest();
      sendRequestToSlave(slave.getSlaveLocator(), req);

   }
   
   protected void loop(List slaves, long duration) throws Throwable
   {
      long start = System.currentTimeMillis();
      while (true)
      {
         Thread.sleep(3000);
         
         if (System.currentTimeMillis() - start >= duration)
         {
            break;
         }
         
         Iterator iter = slaves.iterator();
         
         while (iter.hasNext())
         {
             SlaveMetadata slave = (SlaveMetadata)iter.next();
             
             GetDataRequest req = new GetDataRequest();
             
             List results = (List)sendRequestToSlave(slave.getSlaveLocator(), req);
             
             Iterator iter2 = results.iterator();
             
             while (iter2.hasNext())
             {
                RunData data = (RunData)iter2.next();
                data.getResults(persistor);
                
                String jobName = data.jobName;
                
                Iterator iter3 = slave.getJobs().iterator();
                while (iter3.hasNext())
                {
                   Job job = (Job)iter3.next();
                   if (job.getName().equals(jobName))
                   {
                      ((ResultSource)job).getResults(persistor);
                      break;
                   }
                }
                persistor.persist();
                
             }
             
         }
      }
   }
   
   protected void run()
   {
      try
      {
        init();
        
        Iterator iter = tests.iterator();
        while (iter.hasNext())
        {
           Test test = (Test)iter.next();
           
           Configuration config = (Configuration)configs.get(test.getConfigName());
           
           if (config == null)
           {
              throw new DeploymentException("Unknown config " + test.getConfigName());
           }
           
           List slaves = config.getSlaves();
           Iterator iter2 = slaves.iterator();
           while (iter2.hasNext())
           {
              SlaveMetadata slave = (SlaveMetadata)iter2.next();
              startJobsAtSlave(slave);
           }
           
           loop(slaves, test.getDuration());
           
           slaves = config.getSlaves();
           iter2 = slaves.iterator();
           while (iter2.hasNext())
           {
              SlaveMetadata slave = (SlaveMetadata)iter2.next();
              stopJobsAtSlave(slave);
           }
           
        }
        persistor.close();
      }
      catch (Throwable e)
      {
         log.error("Failure in controller", e);
      }

   }
   
}
