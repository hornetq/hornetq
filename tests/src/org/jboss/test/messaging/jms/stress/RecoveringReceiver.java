/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.test.messaging.jms.stress;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.jboss.logging.Logger;

/**
 * 
 * A RecoveringReceiver.
 * 
 * A Receiver that receives messages from a destination and alternately
 * acknowledges and recovers the session.
 * Must be used with ack mode CLIENT_ACKNOWLEDGE
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RecoveringReceiver extends Receiver
{
   private static final Logger log = Logger.getLogger(RecoveringReceiver.class);
   
   protected int ackSize;
   protected int recoverSize;

   class Count
   {      
      int lastAcked;
      int lastReceived;
   }
    
   public RecoveringReceiver(Session sess, MessageConsumer cons,
         int numMessages, int ackSize, int recoverSize, boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.ackSize = ackSize;
      this.recoverSize = recoverSize;    
   }
   
   public void run()
   {      
      //Small pause so as not to miss any messages in a topic
      try
      {
         Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {         
      }
      
      try
      {      
         int iterations = numMessages / ackSize;

         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            Message m = null;
            for (int innerCount = 0; innerCount < ackSize; innerCount++)
            {
               m = getMessage();
            
               if (m == null)
               {
                  log.error("Message is null");
                  failed = true;
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));
               
           //    log.info("got " + prodName + ":" + msgCount);
               
               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  //First time
                  if (msgCount.intValue() != 0)
                  {
                     log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     failed = true;
                     return;
                  }
                  else
                  {
                     count = new Count();
                     counts.put(prodName, count);
                  }
               }
               else
               {
                  if (count.lastAcked != msgCount.intValue() - 1)
                  {
                     log.error("Message out of sequence for " + prodName + ", expected " + (count.lastAcked + 1));
                     failed = true;
                     return;
                  }
               }
               count.lastAcked = msgCount.intValue();
               
               count.lastReceived = msgCount.intValue();
               
               if (innerCount == ackSize -1)
               {
                  m.acknowledge();
               }
               this.processingDone();
            
            }
            
            if (outerCount == iterations - 1)
            {
               break;
            }
                        
            for (int innerCount = 0; innerCount < recoverSize; innerCount++)
            {
               m = getMessage();
               
               if (m == null)
               {
                  log.error("Message is null");
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");               
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));   
               
           //    log.info("got " + prodName + ":" + msgCount);
               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     failed = true;
                     return;
                  }
                  else
                  {
                     count = new Count();
                     count.lastAcked = -1;
                     counts.put(prodName, count);
                  }                 
               }
               else
               {
                  if (count.lastReceived != msgCount.intValue() - 1)
                  {
                     log.error("Message out of sequence");
                     failed = true;
                     return;
                  }
               }
               count.lastReceived = msgCount.intValue();             
               
               if (innerCount == recoverSize - 1)
               {
                  sess.recover();
               }
               this.processingDone();
            }
         }        
      }
      catch (Exception e)
      {
         log.error("Failed to receive message", e);
         failed = true;
      }
   }

}
