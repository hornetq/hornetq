/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ProducerTest extends AbstractTest
{
   private static final Logger log = Logger.getLogger(ProducerTest.class);

   protected boolean anon;
   
   protected int msgSize;

   protected int deliveryMode;
   
   protected MessageFactory mf;
   
   public Failable createTestThingy()
   {
      return new Sender();
   }

   protected class Sender implements Runnable, Failable
   {
      private boolean failed;   
      
      public void run()
      {
         try
         {
            Thread.sleep(rampDelay);
            
            Connection conn = getNextConnection();
            
            Session sess = conn.createSession(transacted, Session.AUTO_ACKNOWLEDGE); //Ackmode doesn't matter
            
            
            MessageProducer prod = null;
            
            if (anon)
            {
               prod = sess.createProducer(null);
            }
            else
            {
               prod = sess.createProducer(dest);
            }
            
            prod.setDeliveryMode(deliveryMode);
            
            long start = System.currentTimeMillis();
            
            int count = 0;
            
            while (true)
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
               
               if (count != 0 && count % 50 == 0)
               {
                  updateTotalCount(50);
                  count = 0;
               }
               
               if (transacted)
               {
                  if (count % transactionSize == 0)
                  {
                     sess.commit();
                  }
               }
               
               if ((System.currentTimeMillis() - start) >= runLength)
               {
                  break;
               }
            }
                        
            updateTotalCount(count);
            
         }
         catch (Exception e)
         {
            log.error("Sender failed", e);
            failed = true;
         }
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
   
   
   protected boolean parseArgs(String[] args)
   {
      super.parseArgs(args);
      try
      {
         anon = Boolean.parseBoolean(args[9]);
         msgSize = Integer.parseInt(args[10]);
         String messageType = args[11];
         
         if ("javax.jms.Message".equals(messageType))
         {
            mf = new MessageMessageFactory();
         }
         else if ("javax.jms.BytesMessage".equals(messageType))
         {
            mf = new BytesMessageMessageFactory();
         }
         else if ("javax.jms.MapMessage".equals(messageType))
         {
            mf = new MapMessageMessageFactory();
         }
         else if ("javax.jms.ObjectMessge".equals(messageType))
         {
            mf = new ObjectMessageMessageFactory();
         }
         else if ("javax.jms.StreamMessage".equals(messageType))
         {
            mf = new StreamMessageMessageFactory();
         }
         else if ("javax.jms.TextMessage".equals(messageType))
         {
            mf = new TextMessageMessageFactory();
         }
         else if ("foreign".equals(messageType))
         {
            mf = new ForeignMessageMessageFactory();
         }
         else
         {
            printUsage();
            return false;
         }
         
         deliveryMode = Integer.parseInt(args[12]);
         
         log.info("Use anonymous producer? " + anon);
         log.info("Message size: " + msgSize);
         log.info("Message type: " + messageType);
  
         if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT)
         {
            log.error("Invalid delivery mode");
            return false;            
         }
         System.out.println("Delivery Mode:" + (deliveryMode == DeliveryMode.PERSISTENT ? "Persistent" : "Non-persistent"));

         return true;
      }
      catch (Exception e)
      {
         log.error(e);
         printUsage();
         return false;
      }
   }
   
}
