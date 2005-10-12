/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 */
public class Test implements XMLLoadable
{
   private static final Logger log = Logger.getLogger(Test.class);   
   
   protected String name;
   
   protected List slaveInfos;
   
   protected ResultPersistor persistor;
   
   public String getName()
   {
      return name;
   }
  
   public Test(Element el, ResultPersistor persistor) throws ConfigurationException
   {
      slaveInfos = new ArrayList();
      this.persistor = persistor;
      importXML(el);
   }
   
   public void importXML(Element el) throws ConfigurationException
   {
      log.info("Loading test:" + el.getNodeName());
      
      name = MetadataUtils.getElementAttribute(el, "name");
      
      log.info("Test Name is " + name);
      
      Iterator iter = MetadataUtils.getChildrenByTagName(el,"slave");
      
      while (iter.hasNext())
      {
         log.info("Got slave element");
         
         Element slaveElement = (Element)iter.next();
         String slaveName = MetadataUtils.getElementAttribute(slaveElement, "name");
         log.info("Slave name is: " + slaveName);
         Element locatorElement = MetadataUtils.getUniqueChild(slaveElement, "slave-locator");
         String slaveLocator = MetadataUtils.getElementContent(locatorElement);
         SlaveInfo slaveInfo = new SlaveInfo(slaveName, slaveLocator);
         slaveInfos.add(slaveInfo);
         
         iter = MetadataUtils.getChildrenByTagName(slaveElement,"drain-job");      
         while (iter.hasNext())
         {
            Element elChild = (Element)iter.next();
            DrainJob job= new DrainJob(elChild);
            slaveInfo.addJob(job);
            log.info("Added drain job");
         }
         
         iter = MetadataUtils.getChildrenByTagName(slaveElement,"fill-job");      
         while (iter.hasNext())
         {
            Element elChild = (Element)iter.next();
            FillJob job= new FillJob(elChild);
            slaveInfo.addJob(job);
            log.info("Added fill job");
         }
         
         iter = MetadataUtils.getChildrenByTagName(slaveElement,"sender-job");      
         while (iter.hasNext())
         {
            Element elChild = (Element)iter.next();
            SenderJob job= new SenderJob(elChild);
            slaveInfo.addJob(job);
            log.info("Added sender job");
         }
         
         iter = MetadataUtils.getChildrenByTagName(slaveElement, "receiver-job");      
         while (iter.hasNext())
         {
            Element elChild = (Element)iter.next();
            ReceiverJob job= new ReceiverJob(elChild);
            slaveInfo.addJob(job);
            log.info("Added receiver job");
         }
         
      }   
   }
   
   class JobRunner implements Runnable
   {
      private Job job;
      
      private JobResult result;
      
      private String slaveURL;
      
      private Thread t;
      
      public Job getJob()
      {
         return job;
      }
       
      public JobResult getResult()
      {
         return result;
      }

      public JobRunner(Job j, String slaveURL)
      {
         this.job = j;
         this.slaveURL = slaveURL;
      }
      
      public void setThread(Thread t)
      {
         this.t = t;
      }
      
      public Thread getThread()
      {
         return t;
      }
     
      
      public void run()
      {
         try
         {
            RunRequest request = new RunRequest(job);
            result = (JobResult)sendRequestToSlave(slaveURL, request);
         }
         catch (Throwable t)
         {
            log.error("Failed to send job to slave", t);
         }
      }
   }
   
   protected Object sendRequestToSlave(String slaveURL, ServerRequest request) throws Throwable
   {
      InvokerLocator locator = new InvokerLocator(slaveURL);
      Client client = new Client(locator, "perftest");
      Object res = client.invoke(request);
      return res;
   }
   
   public void run()
   {
      log.info("Running test: " + this.name);
      
      List runners = new ArrayList();
      
      Iterator iter = slaveInfos.iterator();
      while (iter.hasNext())
      {
         
         SlaveInfo slaveInfo = (SlaveInfo)iter.next();
         log.info("Starting job(s) at slave: " + slaveInfo.name);
         Iterator jobs = slaveInfo.getJobs().iterator();
         while (jobs.hasNext())
         {
            Job job = (Job)jobs.next();     
            log.info("Starting job: " + job);
            JobRunner runner = new JobRunner(job, slaveInfo.locator);
            Thread t = new Thread(runner);
            runner.setThread(t);
            runners.add(runner);
            t.start();
            log.info("Running job");
         }
      }
      
      iter = runners.iterator();
      while (iter.hasNext())
      {
         JobRunner runner = (JobRunner)iter.next();
         try
         {
            runner.getThread().join();
         }
         catch (InterruptedException e)
         {}
         JobResult result = runner.getResult();
         

         persistor.addValue("testName", this.name);
         persistor.addValue("jobName", runner.getJob().getName());
         runner.getJob().fillInResults(persistor);
         persistor.addValue("messages", result.messages);
         persistor.addValue("time", result.time);
         persistor.persist();
      }
   }
}
