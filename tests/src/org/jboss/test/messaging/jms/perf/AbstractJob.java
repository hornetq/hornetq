
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
 * $Id$
 */
public abstract class AbstractJob implements XMLLoadable, Job, Serializable, ResultSource
{
   private transient static final Logger log = Logger.getLogger(AbstractJob.class);
   
   protected int numConnections;
   
   protected int numSessions;
   
   protected long rampDelay;
   
   protected String destName;
   
   protected boolean transacted;
    
   protected InitialContext ic;
   
   protected Destination dest;
   
   protected Connection[] conns;
   
   protected int connIndex;
   
   protected int totalCount;
   
   protected int transactionSize;
   
   protected String serverURL;
   
   protected boolean running;

   protected abstract Servitor createServitor();
   
   protected Thread[] servitorThreads;
   
   protected Servitor[] servitors;
   
   protected boolean failed;
   
   protected long lastTime;
   
   protected long lastCount;
   
   protected double throughput;
   
   
   public void getResults(ResultPersistor persistor)
   {
      persistor.addValue("numConnections", numConnections);
      persistor.addValue("numSessions", numSessions);
      persistor.addValue("rampDelay", rampDelay);
      persistor.addValue("destName", destName);
      persistor.addValue("transacted", transacted);
      persistor.addValue("transactionSize", transactionSize);
      persistor.addValue("serverURL", serverURL);  
   }
   
   protected synchronized void setRunning(boolean running)
   {
      this.running = running;
   }
   
   public synchronized boolean getRunning()
   {
      return running;
   }
   
   
   public void run()
   {
      try
      {
         log.info("Running job:" + this.getName());
         setup();
         logInfo();
         runTest();
      }
      catch (Exception e)
      {
         log.error("Failed to run test", e);
      }
   }
   
   public void stop()
   {
      log.info("Stopping job: " + this.getName());
      
      for (int i = 0; i < servitors.length; i++)
      {
         servitors[i].stop();
      }
      
      for (int i = 0; i < servitors.length; i++)
      {
         try
         {
            servitorThreads[i].join();
         }
         catch (InterruptedException ignore)
         {     
         }
      }
      log.info("Stopped job");
   }
   
   public RunData getData()
   {
      RunData data = new RunData();
      data.currentTP = throughput;
      data.jobName = getName();
      return data;
   }
   

   public void setTP(double tp)
   {
      
   }
   
   
   protected void runTest() throws Exception
   {
      setRunning(true);
      
      failed = false;
      
      lastTime = System.currentTimeMillis();
      
      try
      {
      
         servitorThreads = new Thread[numSessions];      
         servitors = new Servitor[numSessions];
         
         for (int i = 0; i < numSessions; i++)
         {
            Servitor servitor = createServitor();
            servitorThreads[i] = new Thread(servitor);
            servitorThreads[i].start();
            servitors[i] = servitor;
         }
         
         for (int i = 0; i < numSessions; i++)
         {
            servitorThreads[i].join();
         }

         for (int i = 0; i < numSessions; i++)
         {
            Servitor servitor = servitors[i];
            if (servitor.isFailed())
            {
               failed = true;
            }
         }
      }
      finally
      {
         setRunning(false);
      }
      
   }
   
   protected abstract class AbstractServitor implements Runnable, Servitor
   {
      protected void doThrottle()
      {
         //TODO
      }
   }
   
  
   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }
   
   protected synchronized void updateTotalCount(long count)
   {
      totalCount += count;
      computeTP();
   }
   

   protected void computeTP()
   {
      long now = System.currentTimeMillis();
      
      if (now - lastTime > 1000)
      {     
         throughput = ((double)(1000 * (totalCount - lastCount))) / (now - lastTime);
         
         lastCount = totalCount;
         
         lastTime = now;
      }
   }
   
   protected synchronized int getTotalCount()
   {
      return totalCount;
   }


   protected void setup() throws Exception
   {
      //Hardcoded for now - needs to be configurable if it's to work with non jboss
      //jms providers

      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      env.put("java.naming.provider.url", serverURL);
      env.put("java.naming.factory.url.pkg", "org.jboss.naming:org.jnp.interfaces");
      
      ic = new InitialContext(env);
      
      dest = (Destination)ic.lookup(destName);
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      
      conns = new Connection[numConnections];
      
      for (int i = 0; i < numConnections; i++)
      {
         conns[i] = cf.createConnection();
         conns[i].start();
      }
   }
   

   protected AbstractJob()
   {      
   }
   
   protected void logInfo()
   {
      log.info("JMS Server URL: " + serverURL);
      log.info("Number of connections: " + numConnections);
      log.info("Numbe of concurrent sessions: " + numSessions);
      log.info("Ramp delay: " + rampDelay);
      log.info("JMS destinatoion JNDI name: " + destName);
      log.info("Transacted?: " + transacted);
      log.info("Transaction size: " + transactionSize);
   }
   
   
   public void importXML(Element element) throws DeploymentException
   {  
      this.serverURL = MetadataUtils.getUniqueChildContent(element, "jms-server-url");
      this.numConnections = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "num-connections"));
      this.numSessions = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "num-sessions"));
      this.rampDelay = Long.parseLong(MetadataUtils.getUniqueChildContent(element, "ramp-delay"));
      this.destName = MetadataUtils.getUniqueChildContent(element, "destination-jndi-name");
      this.transacted  = MetadataUtils.getOptionalChildBooleanContent(element, "transacted", false);
      this.transactionSize  = Integer.parseInt(MetadataUtils.getOptionalChildContent(element, "transaction-size", "0"));
   }
   

}