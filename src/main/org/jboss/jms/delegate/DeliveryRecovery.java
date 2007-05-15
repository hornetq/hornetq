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
package org.jboss.jms.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * A DeliveryRecovery
 * 
 * Used for sending information about to recover a delivery to the server
 * on failover
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DeliveryRecovery implements Streamable
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private long deliveryID;
   private long messageID;
   private String queueName;

   // Constructors ---------------------------------------------------------------------------------

   public DeliveryRecovery()
   {
   }

   public DeliveryRecovery(long deliveryID, long messageID, String queueName)
   {
      this.deliveryID = deliveryID;
      this.messageID = messageID;
      this.queueName = queueName;
   }

   // Streamable implementation --------------------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      deliveryID = in.readLong();
      messageID = in.readLong();
      queueName = in.readUTF();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeLong(deliveryID);
      out.writeLong(messageID);
      out.writeUTF(queueName);
   }

   // Public ---------------------------------------------------------------------------------------

   public long getDeliveryID()
   {
      return deliveryID;
   }

   public long getMessageID()
   {
      return messageID;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public String toString()
   {
      return "DeliveryRecovery[ID=" + deliveryID + ", MID=" + messageID + ", Queue name=" + queueName + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
