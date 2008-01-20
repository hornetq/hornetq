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
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.util.id.GUID;

/**
 * 
 * A receiver that receives messages in a XA transaction
 * 
 * Receives <commitSize> messages then prepares, commits, then
 * Receives <rollbackSize> messages then prepares, rollsback until
 * a total of <numMessages> messages have been received (committed)
 * <numMessages> must be a multiple of <commitSize>
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Transactional2PCReceiver extends Receiver
{
   private static final Logger log = Logger.getLogger(Transactional2PCReceiver.class);
   
   protected int commitSize;
   protected int rollbackSize;
   
   protected XAResource xaResource;
   
   class Count
   {      
      int lastCommitted;
      int lastReceived;
   }
    
   public Transactional2PCReceiver(XASession sess, MessageConsumer cons, int numMessages,
         int commitSize, int rollbackSize, boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;   
      this.xaResource = sess.getXAResource();
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
         
         MessagingXid xid = null;
         
         xid = new MessagingXid("bq1".getBytes(), 1, new GUID().toString().getBytes());
         xaResource.start(xid, XAResource.TMNOFLAGS);
         
         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
                                                
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = getMessage();
                        
               if (m == null)
               {
                  log.error("Message is null");
                  setFailed(true);
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));
               
               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  //First time
                  if (msgCount.intValue() != 0)
                  {
                     log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     setFailed(true);
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
                     log.error("Message out of sequence for " + prodName + ", expected " + (count.lastCommitted + 1) + ", actual " + msgCount);
                     setFailed(true);
                     return;
                  }
               }
               count.lastCommitted = msgCount.intValue();
               
               count.lastReceived = msgCount.intValue();
               
               if (innerCount == commitSize -1)
               {
                  xaResource.end(xid, XAResource.TMSUCCESS);
                  xaResource.prepare(xid);
                  xaResource.commit(xid, false);
                                    
                  //Starting new tx
                  xid = new MessagingXid("bq1".getBytes(), 1, new GUID().toString().getBytes());
                  xaResource.start(xid, XAResource.TMNOFLAGS);
                 
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
                  log.error("Message is null (rollback)");
                  setFailed(true);
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");               
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));
                    
               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     setFailed(true);
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
                     setFailed(true);
                     return;
                  }
               }
               count.lastReceived = msgCount.intValue();          
               
               if (innerCount == rollbackSize -1)
               {
                  xaResource.end(xid, XAResource.TMSUCCESS);
                  xaResource.prepare(xid);
                  xaResource.rollback(xid);
                  
                  xid = new MessagingXid("bq1".getBytes(), 1, new GUID().toString().getBytes());
                  xaResource.start(xid, XAResource.TMNOFLAGS);
               }
               processingDone();
            }            
         }
         
         xaResource.end(xid, XAResource.TMSUCCESS);
         xaResource.prepare(xid);
         xaResource.commit(xid, false);
         
         finished();
           
      }
      catch (Exception e)
      {
         log.error("Failed to receive message", e);
         setFailed(true);
      }
   }
}
