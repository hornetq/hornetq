
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class AbstractTest
{
   private static final Logger log = Logger.getLogger(AbstractTest.class);
   
   protected int numConnections;
   
   protected int numSessions;
   
   protected long rampDelay;
   
   protected long runLength;
   
   protected String destName;
   
   protected boolean transacted;
    
   protected InitialContext ic;
   
   protected Destination dest;
   
   protected Connection[] conns;
   
   protected int connIndex;
   
   protected int totalCount;
   
   protected int transactionSize;
   
   protected String serverURL;

   protected abstract Failable createTestThingy();
   
   public void run(String[] args)
   {
      if (parseArgs(args))
      {
         try
         {
            setup();
            runTest();
         }
         catch (Exception e)
         {
            log.error("Failed to run test", e);
         }
      }
   }
   
   protected boolean runTest() throws Exception
   {
      Thread[] threads = new Thread[numSessions];      
      Failable[] thingies = new Failable[numSessions];
      
      Thread reporter = new Thread(new Reporter());
      reporter.start();
      
      for (int i = 0; i < numSessions; i++)
      {
         Failable thingy = createTestThingy();
         threads[i] = new Thread(thingy);
         threads[i].start();
         thingies[i] = thingy;
      }
      
      for (int i = 0; i < numSessions; i++)
      {
         threads[i].join();
      }
      reporter.join();
      
      for (int i = 0; i < numSessions; i++)
      {
         Failable thingy = thingies[i];
         if (thingy.isFailed())
         {
            return false;
         }
      }
      return true;
      
   }
   
   protected class Reporter implements Runnable
   {
      public void run()
      {
         try
         {
            long start = System.currentTimeMillis();
            
            int lastCount = 0;
            
            long lastTime = start;
            
            double minThroughput = 0;
            
            double maxThroughput = 0;
            
            while (true)
            {
               Thread.sleep(3000);
               
               long now = System.currentTimeMillis();
               
               int totalCount = getTotalCount();
               
               double averageThroughput = 1000 * totalCount / (now - start);
               double currentThroughput = 1000 * (totalCount - lastCount) / (now - lastTime);
               
               lastCount = totalCount;
               lastTime = now;
               
               if (currentThroughput < minThroughput)
               {
                  minThroughput = currentThroughput;
               }
               
               if (currentThroughput > maxThroughput)
               {
                  maxThroughput = currentThroughput;
               }
                              
               log.info("===================================");
               log.info("Current:" + currentThroughput + " msgs/s");
               log.info("Peak:" + maxThroughput + " msgs/s");
               log.info("Minimum:" + minThroughput + " msgs/s");
               log.info("Average:" + averageThroughput + " msgs/s");
               
               if ((System.currentTimeMillis() - start) >= runLength)
               {
                  break;
               }
               
            }
         }
         catch (Exception e)
         {
            log.error("Reporter failed", e);
         }
      }
   }
   
   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }
   
   protected synchronized void updateTotalCount(int count)
   {
      totalCount += count;
   }
   
   protected synchronized int getTotalCount()
   {
      return totalCount;
   }


   protected void setup() throws Exception
   {
      //Hardcoded for now

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
   
   protected boolean parseArgs(String[] args)
   {
      try
      {
         serverURL = args[1];
         numConnections = Integer.parseInt(args[2]);
         numSessions = Integer.parseInt(args[3]);
         rampDelay = Long.parseLong(args[4]);
         runLength = Long.parseLong(args[5]);
         destName = args[6];
         transacted = Boolean.parseBoolean(args[7]);
         transactionSize = Integer.parseInt(args[8]);

         log.info("Running consumer test");
         log.info("Server URL: " + serverURL);
         log.info("Number of connections: " + numConnections);
         log.info("Number of sessions: " + numSessions);
         log.info("Ramp up delay (ms): " + rampDelay);
         log.info("Run length: " + runLength + " ms (" + (runLength / (1000 * 60)) + " minutes)");
         log.info("Destination: " + dest);
         log.info("Transacted: " + transacted);
         log.info("Transaction size: " + transactionSize);
         
         return true;
      }
      catch (Exception e)
      {
         log.error(e);
         printUsage();
         return false;
      }
   }
   
   protected void printUsage()
   {
      //TODO
      log.info("Invalid args");
   }
}