/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.perf.framework.factories.MessageFactory;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SenderJob extends BaseThroughputJob
{
   private static final long serialVersionUID = -4031253412475892666L;

   private transient static final Logger log = Logger.getLogger(SenderJob.class);

   protected boolean anon;
   
   protected int msgSize;

   protected int deliveryMode;
   
   protected MessageFactory mf;
   
   protected long initialPause;
   
   public Servitor createServitor(int numMessages)
   {
      return new Sender(numMessages);
   }
   
   protected void logInfo()
   {
      super.logInfo();
      log.trace("Use anonymous producer? " + anon);
      log.trace("Message size: " + msgSize);
      log.trace("Message type: " + mf.getClass().getName());
      log.trace("Delivery Mode:" + (deliveryMode == DeliveryMode.PERSISTENT ? "Persistent" : "Non-persistent"));
      log.trace("Initial pause:" + initialPause);
   }
   
   public SenderJob(String slaveURL, Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize, 
         int numMessages, boolean anon, int messageSize,
         MessageFactory messageFactory, int deliveryMode, long initialPause)
   {
      super (slaveURL, jndiProperties, destinationName, connectionFactoryJndiName, numConnections,
            numSessions, transacted, transactionSize, numMessages);
      this.anon = anon;
      this.msgSize = messageSize;
      this.mf = messageFactory;
      this.deliveryMode = deliveryMode;
      this.initialPause = initialPause;
   }
   


   protected class Sender extends AbstractServitor
   {
      
      Sender(int numMessages)
      {
         super(numMessages);
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
            throwable = e;
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
            
            Thread.sleep(initialPause);
         }
         catch (Throwable e)
         {
            log.error("Failed to init", e);
            throwable = e;
            failed = true;
         }
      }
      
      public void run()
      {
         try
         {
            int count = 0;
            
            while (count < numMessages)
            {               
            
               Message m = mf.getMessage(sess, msgSize);
               
               if (anon)
               {
                  prod.send(dest, m);            
               }
               else
               {
                  prod.send(m);
               }
               
               count++;
           
               if (transacted)
               {                  
                  if (count % transactionSize == 0)
                  {
                     sess.commit();
                  }
               }                   
            }

         }
         catch (Throwable e)
         {
            log.error("Sender failed", e);
            throwable = e;
            failed = true;
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
   
   public void setInitialPause(long initialPause)
   {
      this.initialPause = initialPause;
   }
}
