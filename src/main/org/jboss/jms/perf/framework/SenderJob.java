/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.perf.framework.factories.MessageFactory;
import org.jboss.logging.Logger;

/**
 * 
 * A SenderJob.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SenderJob extends BaseThroughputJob
{
   private static final long serialVersionUID = -4031253412475892666L;

   private transient static final Logger log = Logger.getLogger(SenderJob.class);

   protected boolean anon;
   
   protected int msgSize;

   protected int deliveryMode;
   
   protected MessageFactory mf;
   
   protected double targetRate;
   
   double targetMsgsPerSamplePeriod;
   
   private static final long SAMPLE_PERIOD = 50;
      
   public Servitor createServitor(long testTime)
   {
      return new Sender(testTime);
   }
  
   public SenderJob(Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize, 
         long testTime, boolean anon, int messageSize,
         MessageFactory messageFactory, int deliveryMode, double targetRate)
   {
      super (jndiProperties, destinationName, connectionFactoryJndiName, numConnections,
            numSessions, transacted, transactionSize, testTime);
      this.anon = anon;
      this.msgSize = messageSize;
      this.mf = messageFactory;
      this.deliveryMode = deliveryMode;
      this.targetRate = targetRate;
      
      targetMsgsPerSamplePeriod = (targetRate * SAMPLE_PERIOD) / 1000;
      
      //log.info("target msgs per sample period:" + targetMsgsPerSamplePeriod);
   }
   
   protected class Sender extends AbstractServitor
   {     
      Sender(long testTime)
      {
         super(testTime);
         
         //log.info("testTime:" + testTime);
      }
      
      MessageProducer prod;
      
      Session sess;
      
      public void deInit()
      {
         log.info("De-initialising");
         try
         {
            sess.close();
         }      
         catch (Throwable e)
         {
            log.error("Failed to deInit()", e);
            failed = true;
         }
      }
      
      public void init()
      {
         log.info("Initialising");
         try
         {
            Connection conn = getNextConnection();
            
            sess = conn.createSession(transacted, Session.AUTO_ACKNOWLEDGE); //Ackmode doesn't matter            
            
            prod = null;
            
            if (anon)
            {
               prod = sess.createProducer(null);
            }
            else
            {
               prod = sess.createProducer(dest);
            }
            
            prod.setDeliveryMode(deliveryMode);             
         }
         catch (Throwable e)
         {
            log.error("Failed to init", e);
            failed = true;
         }
      }
      
      public void run()
      {
         try
         {
            long start = System.currentTimeMillis();
            
            long now = 0;
            
            while (true)
            {
               now = System.currentTimeMillis();
               
               if (now > start + testTime)
               {                                    
                  //Done
                  
                  break;
               }
               
               Message m = mf.getMessage(sess, msgSize);                            
               
               if (anon)
               {
                  prod.send(dest, m);            
               }
               else
               {
                  prod.send(m);
               }
               
               if (transacted)
               {                  
                  if (messageCount % transactionSize == 0)
                  {
                     sess.commit();
                  }
               }  
               
               messageCount++;
               
               doThrottle(now);
                             
            }
                                     
            actualTime = now - start;
            
            log.info("Actual time is: " + actualTime);
            
         }
         catch (Throwable e)
         {
            log.error("Sender failed", e);
            failed = true;
         }
      }
      
      private long lastTime = 0;
      
      private int lastCount = 0;
                  
      protected void doThrottle(long now)
      {
         if (lastTime != 0)
         {
            long elapsedTime = now - lastTime;
            
            if (elapsedTime >= SAMPLE_PERIOD)
            {
               long msgs = messageCount - lastCount;
               
               //log.info("elapsedTime is " + elapsedTime);
               //log.info("msgs:" + msgs);
               
               double targetMsgs = targetRate * (double)elapsedTime / 1000;       
               
               //log.info("Target msgs:" + targetMsgs);
               
               if (msgs > targetMsgs)
               {
                  //Need to throttle
                  //log.info("throttling");
                  
                  long throttleTime = (long)((1000 * (double)msgs / targetRate) - elapsedTime);
                                    
                  //log.info("Throttle time is:" + throttleTime);
                  try
                  {
                     Thread.sleep(throttleTime);
                  }
                  catch (InterruptedException e)
                  {
                     //Ignore
                  }
               }
               
               lastTime = now;
               
               lastCount = messageCount;
            }
         }
         else
         {
            lastTime = now;
         }
      }

   } 
   

   /**
    * Set the anon.
    * 
    * @param anon The anon to set.
    */
   public void setAnon(boolean anon)
   {
      this.anon = anon;
   }

   /**
    * Set the deliveryMode.
    * 
    * @param deliveryMode The deliveryMode to set.
    */
   public void setDeliveryMode(int deliveryMode)
   {
      this.deliveryMode = deliveryMode;
   }

   /**
    * Set the mf.
    * 
    * @param mf The mf to set.
    */
   public void setMf(MessageFactory mf)
   {
      this.mf = mf;
   }

   /**
    * Set the msgSize.
    * 
    * @param msgSize The msgSize to set.
    */
   public void setMsgSize(int msgSize)
   {
      this.msgSize = msgSize;
   }
   
}
