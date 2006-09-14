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
package org.jboss.jms.server.endpoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.util.Streamable;

/**
 * 
 * A ClientDelivery
 * Encapsulates a delivery of some messages to a client consumer
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ClientDelivery implements Streamable
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private List msgs;
         
   private int consumerID;
    
   // Constructors --------------------------------------------------
   
   public ClientDelivery()
   {      
   }

   public ClientDelivery(List msgs, int consumerID)
   {
      this.msgs = msgs;
      
      this.consumerID = consumerID;      
   }
  
   // Streamable implementation
   // ---------------------------------------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(consumerID);
      
      out.writeInt(msgs.size());
      
      Iterator iter = msgs.iterator();
      
      while (iter.hasNext())
      {
         MessageProxy mp = (MessageProxy)iter.next();
         
         out.writeByte(mp.getMessage().getType());

         out.writeInt(mp.getDeliveryCount());

         mp.getMessage().write(out);
      }      
   }

   public void read(DataInputStream in) throws Exception
   {
      consumerID = in.readInt();
      
      int numMessages = in.readInt();
      
      msgs = new ArrayList(numMessages);
      
      for (int i = 0; i < numMessages; i++)
      {
         byte type = in.readByte();
         
         int deliveryCount = in.readInt();
         
         JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);

         m.read(in);

         MessageProxy md = JBossMessage.createThinDelegate(m, deliveryCount);
         
         msgs.add(md);
      }      
   }

   // Public --------------------------------------------------------
   
   public List getMessages()
   {
      return msgs;
   }
   
   public int getConsumerID()
   {
      return consumerID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
