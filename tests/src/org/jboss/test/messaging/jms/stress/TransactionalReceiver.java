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
 * A Receiver that receives messages from a destination in a JMS transaction
 * 
 * Receives <commitSize> messages then commits, then
 * Receives <rollbackSize> messages then rollsback until
 * a total of <numMessages> messages have been received (committed)
 * <nuMessages> must be a multiple of <commitSize>
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * TransactionalReceiver.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class TransactionalReceiver extends Receiver
{
   private static final Logger log = Logger.getLogger(TransactionalReceiver.class);
   
   protected int commitSize;
   protected int rollbackSize;
   
   class Count
   {      
      int lastCommitted;
      int lastReceived;
   }
    
   public TransactionalReceiver(Session sess, MessageConsumer cons, int numMessages,
         int commitSize, int rollbackSize, boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;   
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
         int iterations = numMessages / commitSize;

         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = getMessage();
                           
               if (m == null)
               {
                  log.error("Message is null");
                  failed = true;
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = Integer.valueOf(m.getIntProperty("MSG_NUMBER"));  
               
               log.trace(this + "Received message(commit): " + prodName + ":" + msgCount);
               
               
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
                  if (count.lastCommitted != msgCount.intValue() - 1)
                  {
                     log.error("Message out of sequence for " + m.getJMSMessageID() + " " + prodName + ", expected " + (count.lastCommitted + 1) + ", actual " + msgCount);
                     failed = true;
                     return;
                  }
               }
               count.lastCommitted = msgCount.intValue();
               
               count.lastReceived = msgCount.intValue();
               
               if (innerCount == commitSize -1)
               {
                  log.trace("Committing");
                  sess.commit();
               }
               
               processingDone();
            
            }            
            
            
            if (outerCount == iterations - 1)
            {
               break;
            }
                        
            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = getMessage();
               
               if (m == null)
               {
                  log.error("Message is null");
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");               
               Integer msgCount = Integer.valueOf(m.getIntProperty("MSG_NUMBER"));
               
               log.trace("Received message(rollback): " + prodName + ":" + msgCount);
               
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
                     count.lastCommitted = -1;
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
               
               if (innerCount == rollbackSize -1)
               {
                  log.trace("Rolling back");
                  sess.rollback();
               }
               processingDone();
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
