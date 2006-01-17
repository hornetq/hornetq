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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.logging.Logger;

/**
 * 
 * A Receiver.
 * Receives messages from a dstination for stress testing
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * Receiver.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class Receiver extends Runner implements MessageListener
{
   private static final Logger log = Logger.getLogger(Receiver.class);
   
   protected MessageConsumer cons;
   
   protected int count;
   
   protected boolean isListener;
   
   protected Map counts = new HashMap();
   
   public Receiver(Session sess, MessageConsumer cons, int numMessages, boolean isListener) throws Exception
   {
      super(sess, numMessages);
      this.cons = cons;
      this.isListener = isListener;
      if (this.isListener)
      {
         cons.setMessageListener(this);
      }
   }
   
   protected volatile Message theMessage;
   
   public void onMessage(Message m)
   {
      try
      {
         theMessage = m;
         
         //Wait for message to be processed
         synchronized (lock)
         {
            lock.wait();
         }
         
      }
      catch (Exception e)
      {
         log.error("Failed to put in channel", e);
         failed = true;
      }
   }
   
   private Object lock = new Object();
   
   protected Message getMessage() throws Exception
   {
      if (isListener)
      {
         long start = System.currentTimeMillis();
         Message m = null;
         while (System.currentTimeMillis() - start < 1000)
         {
            if (theMessage != null)
            {
               m = theMessage;
               theMessage = null;
               break;
            }
            Thread.yield();
         }
         return m;
         
      }
      else
      {
         return cons.receive(1000);
      }
   }
   
   protected void processingDone()
   {
      if (isListener)
      {
         synchronized (lock)
         {
            lock.notify();
         }
      }
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
         while (count < numMessages)
         {
            Message m = getMessage();
            
            if (m == null)
            {
               log.error("Message is null");
               failed = true;
               processingDone();
               return;
            }
            String prodName = m.getStringProperty("PROD_NAME");
            Integer msgCount = Integer.valueOf(m.getIntProperty("MSG_NUMBER"));
                      
            Integer prevCount = (Integer)counts.get(prodName);
            if (prevCount == null)
            {
               if (msgCount.intValue() != 0)
               {
                  log.error("First message received not zero");
                  failed = true;
                  processingDone();
                  return;
               }               
            }
            else
            {
               if (prevCount.intValue() != msgCount.intValue() - 1)
               {
                  log.error("Message out of sequence");
                  failed = true;
                  processingDone();
                  return;
               }
            }
            counts.put(prodName, msgCount);
            
            count++;
            
            processingDone();
         }
         
      }
      catch (Exception e)
      {
         log.error("Failed to receive message", e);
         failed = true;
      }
   }

}

