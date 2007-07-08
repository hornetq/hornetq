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

import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.impl.message.MessageFactory;

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
   // Constants ------------------------------------------------------------------------------------
   
   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   private Message msg;
         
   private String consumerId;
   
   private long deliveryId;
   
   private int deliveryCount;
    
   // Constructors ---------------------------------------------------------------------------------
   
   public ClientDelivery()
   {      
   }

   public ClientDelivery(Message msg, String consumerId, long deliveryId, int deliveryCount)
   {
      super (PacketSupport.CLIENT_DELIVERY);
      
      this.msg = msg;
      
      this.consumerId = consumerId;
      
      this.deliveryId = deliveryId;
      
      this.deliveryCount = deliveryCount;
   }
         
   // Streamable implementation --------------------------------------------------------------------

   public void write(DataOutputStream out) throws Exception
   {

      super.write(out);
      
      out.writeUTF(consumerId);

      out.writeInt(deliveryCount);
 
      out.writeLong(deliveryId);
   
      out.writeByte(msg.getType());
  
      msg.write(out);   

      out.flush();
   }

   public void read(DataInputStream in) throws Exception
   {
      consumerId = in.readUTF();
      
      deliveryCount = in.readInt();
       
      deliveryId = in.readLong();
      
      byte type = in.readByte();
                
      msg = MessageFactory.createMessage(type);
      
      msg.read(in);
   }

   // Public ---------------------------------------------------------------------------------------
   
   public Message getMessage()
   {
      return msg;
   }
   
   public String getConsumerId()
   {
      return consumerId;
   }
   
   public long getDeliveryId()
   {
      return deliveryId;
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   public String toString()
   {
      return "ClientDelivery[" + msg + "]";
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------   
}
