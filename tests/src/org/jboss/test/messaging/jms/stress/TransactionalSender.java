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
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A Sender that sends messages to a destination in a JMS transaction.
 *
 * Sends messages to a destination in a jms transaction.
 * Sends <commitSize> messages then commits, then
 * sends <rollbackSize> messages then rollsback until
 * a total of <numMessages> messages have been sent (commitSize)
 * <numMessages> must be a multiple of <commitSize>
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TransactionalSender extends Sender
{
   private static final Logger log = Logger.getLogger(TransactionalSender.class);
   
   protected int commitSize;
   
   protected int rollbackSize;
   
   public TransactionalSender(String prodName, Session sess, MessageProducer prod, int numMessages,
         int commitSize, int rollbackSize)
   {
      super(prodName, sess, prod, numMessages);

      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;
      
   }
   
   public void run()
   {    
      int iterations = numMessages / commitSize;

      try
      {
         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", outerCount * commitSize + innerCount);   
               prod.send(m);
            }
            sess.commit();
            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", (outerCount + 1) * commitSize + innerCount);          
               prod.send(m);
            }
            sess.rollback();           
         }
      }
      catch (Exception e)
      {
         log.error("Failed to send message", e);
         setFailed(true);
      }
   }
}
