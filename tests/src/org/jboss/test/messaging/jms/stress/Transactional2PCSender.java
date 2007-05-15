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
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.jboss.jms.tx.MessagingXid;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

/**
 * 
 * A Sender that sends messages to a destination in an XA transaction
 * 
 * Sends messages to a destination in a jms transaction.
 * Sends <commitSize> messages then prepares, commits, then
 * sends <rollbackSize> messages then prepares, rollsback until
 * a total of <numMessages> messages have been sent (commitSize)
 * <nuMessages> must be a multiple of <commitSize>
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Transactional2PCSender extends Sender
{
   private static final Logger log = Logger.getLogger(Transactional2PCSender.class);
   
   protected int commitSize;
   
   protected int rollbackSize;
   
   protected XAResource xaResource;
   
   public Transactional2PCSender(String prodName, XASession sess, MessageProducer prod, int numMessages,
         int commitSize, int rollbackSize)
   {
      super(prodName, sess, prod, numMessages);

      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;
      this.xaResource = sess.getXAResource();
   }
   
   public void run()
   {    

      
      int iterations = numMessages / commitSize;

      try
      {
         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            MessagingXid xid = null;
            if (commitSize > 0)
            {
               xid = new MessagingXid("bq1".getBytes(), 1, new GUID().toString().getBytes());
               xaResource.start(xid, XAResource.TMNOFLAGS);
            }
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", outerCount * commitSize + innerCount);   
               prod.send(m);
            }
            if (commitSize > 0)
            {
               xaResource.end(xid, XAResource.TMSUCCESS);
               xaResource.prepare(xid);
               xaResource.commit(xid, false);
            }
            if (rollbackSize > 0)
            {
               xid = new MessagingXid("bq1".getBytes(), 1, new GUID().toString().getBytes());
               xaResource.start(xid, XAResource.TMNOFLAGS);
            }
            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", (outerCount + 1) * commitSize + innerCount);          
               prod.send(m);
            }
            if (rollbackSize > 0)
            {
               xaResource.end(xid, XAResource.TMSUCCESS);
               xaResource.prepare(xid);
               xaResource.rollback(xid);
            }           
         }
      }
      catch (Exception e)
      {
         log.error("Failed to send message", e);
         failed = true;
      }
   }
}

