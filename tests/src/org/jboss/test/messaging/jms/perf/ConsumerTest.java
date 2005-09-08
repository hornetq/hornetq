
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsumerTest extends AbstractTest
{
   private static final Logger log = Logger.getLogger(ProducerTest.class);

   protected int ackMode;
   
   protected String subName;
   
   protected String selector;
   
   protected boolean noLocal;
   
   protected boolean asynch;
   
   
   public Failable createTestThingy()
   {
      return new Receiver();
   }

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
            log.error("Failed to run performance test", e);
         }
      }
   }
   
   
   protected class Receiver implements Runnable, Failable
   {
      private boolean failed;   
      
      public void run()
      {
         try
         {
            log.info("Running receiver");
            
            Thread.sleep(rampDelay);
            
            Connection conn = getNextConnection();
          
            Session sess = conn.createSession(transacted, ackMode);
           
            MessageConsumer cons = sess.createConsumer(dest, selector, noLocal);
            
            long start = System.currentTimeMillis();
            
            int count = 0;
            
            while (true)
            {
            
               Message m = cons.receive(1000);     
                      
               if (m != null)
               {
                  count++;
               }
               
               if (count != 0 && count % 10 == 0)
               {
                  updateTotalCount(10);
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
            log.error("Receiver failed", e);
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
         
         ackMode = Integer.parseInt(args[9]);
         
         subName = args[10];
         if (subName.equals("NULL"))
         {
            subName = null;
         }
         
         selector = args[11];
         if (selector.equals("NULL"))
         {
            selector = null;
         }
         
         noLocal = Boolean.parseBoolean(args[12]);
         
         asynch = Boolean.parseBoolean(args[13]);
                 
         
         log.info("Acknowledgement Mode? " + ackMode);
         log.info("Durable subscription name: " + subName);
         log.info("Message selector: " + selector);
         log.info("No local?: " + noLocal);
         log.info("Asynchonous receive? " + asynch);
  
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