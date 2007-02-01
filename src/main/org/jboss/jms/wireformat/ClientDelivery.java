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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * 
 * A ClientDelivery
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ClientDelivery extends CallbackSupport
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private MessageProxy msg;
         
   private int consumerId;
    
   // Constructors --------------------------------------------------
   
   public ClientDelivery()
   {      
   }

   public ClientDelivery(MessageProxy msg, int consumerId)
   {
      super (PacketSupport.CLIENT_DELIVERY);
      
      this.msg = msg;
      
      this.consumerId = consumerId;
   }
         
   // Streamable implementation
   // ---------------------------------------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      out.writeInt(consumerId);
      
      out.writeByte(msg.getMessage().getType());

      out.writeInt(msg.getDeliveryCount());
      
      out.writeLong(msg.getDeliveryId());

      msg.getMessage().write(out);          
      
      out.flush();
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
      
      consumerId = in.readInt();
      
      byte type = in.readByte();
      
      int deliveryCount = in.readInt();
      
      long deliveryId = in.readLong();
      
      JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);

      m.read(in);

      msg = JBossMessage.createThinDelegate(deliveryId, m, deliveryCount); 
   }

   // Public --------------------------------------------------------
   
   public MessageProxy getMessage()
   {
      return msg;
   }
   
   public int getConsumerId()
   {
      return consumerId;
   }

   public String toString()
   {
      return "ClientDelivery[" + msg + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
