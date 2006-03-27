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

import org.jboss.logging.Logger;

/**
 * 
 * A Sender.
 * 
 * Sends messages to a destination, used in stress testing
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * Sender.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class Sender extends Runner
{
   private static final Logger log = Logger.getLogger(Sender.class);
   
   protected MessageProducer prod;
   
   protected String prodName;
   
   protected int count;
   
   public Sender(String prodName, Session sess, MessageProducer prod, int numMessages)
   {
      super(sess, numMessages);
      this.prod = prod;
      this.prodName = prodName;
   }
   
   public void run()
   {
      try
      {
         while (count < numMessages)
         {
            Message m = sess.createMessage();
            m.setStringProperty("PROD_NAME", prodName);
            m.setIntProperty("MSG_NUMBER", count);
            prod.send(m);
        //    log.info("sent: " + prodName + ":" + count +" id:" + m.getJMSMessageID());
            count++;
         }
      }
      catch (Exception e)
      {
         log.error("Failed to send message", e);
         failed = true;
      }
   }

}
