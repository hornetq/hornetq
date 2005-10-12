
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 */
public abstract class BaseThroughputJob extends BaseJob implements XMLLoadable
{
   private transient static final Logger log = Logger.getLogger(BaseThroughputJob.class);
   
   /* Number of Connections to use */
   protected int numConnections;
   
   /* Number of concurrent session to use - sessions use connections in round-robin fashion */
   protected int numSessions;
   
   /* How long to wait (ms) before starting next session when starting up */
   protected long rampDelay;
     
   /* Is the session transacted? */
   protected boolean transacted;
    
   /* Array of connections to use */
   protected Connection[] conns;
   
   protected int connIndex;
   
   protected int transactionSize;
   
   protected abstract Servitor createServitor();
    
   protected boolean failed;
   
   /* How long to wait before gathering results proper */
   protected long warmUpTime;
   
   protected long coolDown;
   
   /* How long to run for after warm up */
   protected long runTime;
   
   protected long throttle;
   
   protected long throttleScale;
   
   public BaseThroughputJob()
   {
      
   }
   
   public BaseThroughputJob(Element e) throws ConfigurationException
   {
      super(e);
      if (log.isTraceEnabled()) { log.trace("Constructing BaseThroughputJob from XML element"); }
   }
   
   public JobResult run()
   {
      try
      {
         log.info("Running job:" + this.getName());
         setup();
         logInfo();
         JobResult result = runTest();
         tearDown();
         return result;
      }
      catch (Exception e)
      {
         log.error("Failed to run test", e);
         return null;
      }
   }
   
     
   protected JobResult runTest() throws Exception
   {
      failed = false;
      
      Thread[] servitorThreads = new Thread[numSessions];      
      Servitor[] servitors = new Servitor[numSessions];
      
      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = createServitor();
         servitorThreads[i] = new Thread(servitor);
         servitorThreads[i].start();
         servitors[i] = servitor;
         Thread.sleep(rampDelay);
      }
      
      Thread.sleep(warmUpTime);
      
      long startTime = System.currentTimeMillis();
      
      for (int i = 0; i < numSessions; i++)
      {
         servitors[i].setCounting(true);
      }
      
      Thread.sleep(runTime);
      
      for (int i = 0; i < numSessions; i++)
      {
         servitors[i].setCounting(false);
      }
      
      Thread.sleep(coolDown);
      
      for (int i = 0; i < numSessions; i++)
      {
         servitors[i].stop();
      }
      
      for (int i = 0; i < numSessions; i++)
      {
         servitorThreads[i].join();
      }
      
      long endTime = System.currentTimeMillis();
      

      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = servitors[i];
         if (servitor.isFailed())
         {
            failed = true;
         }
      }
      
      
      if (!failed)
      {
         long totalCount = 0;
         for (int i = 0; i < numSessions; i++)
         {
            totalCount += servitors[i].getCount();
         }
         
         JobResult jr = new JobResult(totalCount, endTime - startTime);
         
         return jr;
      }
      
      return null;
         
   }
   
   abstract class AbstractServitor implements Servitor
   {
      protected boolean failed; 
      
      protected long count;
      
      protected boolean stopping;
      
      protected long startCount;
      
      protected long endCount;
      
      public void stop()
      {
         stopping = true;
      }
      
      public void setCounting(boolean counting)
      {
         if (counting)
         {
            startCount = count;            
         }
         else
         {
            endCount = count;
         }
      }
      
      public long getCount()
      {
         return endCount - startCount;
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
   
   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }
     
   protected void setup() throws Exception
   {
      super.setup();
      
      conns = new Connection[numConnections];
      
      for (int i = 0; i < numConnections; i++)
      {
         conns[i] = cf.createConnection();
         conns[i].start();
      }
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      for (int i = 0; i < numConnections; i++)
      {         
         conns[i].close();
      }
   }
   

   protected void logInfo()
   {
      super.logInfo();      
      log.info("Number of connections: " + numConnections);
      log.info("Numbe of concurrent sessions: " + numSessions);
      log.info("Ramp delay: " + rampDelay);
      log.info("Transacted?: " + transacted);
      log.info("Transaction size: " + transactionSize);
      log.info("Warm-up time: " + warmUpTime);
      log.info("Run time: " + runTime);
      log.info("Throttle: " + throttle);
      log.info("Throttle scale: " + throttleScale);
   }
   
   
   public void importXML(Element element) throws ConfigurationException
   {  
      if (log.isTraceEnabled()) { log.trace("importing xml"); }
      super.importXML(element);      
      this.numConnections = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "num-connections"));
      this.numSessions = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "num-sessions"));
      this.rampDelay = Long.parseLong(MetadataUtils.getUniqueChildContent(element, "ramp-delay"));
      this.transacted  = MetadataUtils.getOptionalChildBooleanContent(element, "transacted", false);
      this.transactionSize  = Integer.parseInt(MetadataUtils.getOptionalChildContent(element, "transaction-size", "0"));
      this.warmUpTime = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "warmup-time"));
      this.runTime = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "run-time"));
      this.throttle = Long.parseLong(MetadataUtils.getOptionalChildContent(element, "throttle", "0"));
      this.throttleScale = Long.parseLong(MetadataUtils.getOptionalChildContent(element, "throttle-scale", "1"));
      if (this.throttleScale == 0)
      {
         this.throttleScale = 1;
      }
      this.coolDown = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "dool-down"));
   }
   

   public void fillInResults(ResultPersistor persistor)
   {
      persistor.addValue("numConnections", this.numConnections);
      persistor.addValue("numSessions", this.numSessions);
      persistor.addValue("rampDelay", this.rampDelay);
      persistor.addValue("transacted", this.transacted);
      persistor.addValue("transactionSize", this.transactionSize);
      persistor.addValue("warmUpTime", this.warmUpTime);
      persistor.addValue("coolDown", this.coolDown);
      persistor.addValue("runTime", this.runTime);
      persistor.addValue("throttle", this.throttle);
      persistor.addValue("throttleScale", this.throttleScale);
   }
   

}