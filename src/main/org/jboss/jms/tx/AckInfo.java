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
package org.jboss.jms.tx;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jboss.jms.message.MessageProxy;

/**
 * Struct like class for holding information regarding an acknowledgement to be passed to the server
 * for processing.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox </a>
 *
 * $Id$
 */
public class AckInfo implements Externalizable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -5951156790257302184L;
   
   // Attributes ----------------------------------------------------
   
   protected long messageID;
   
   protected int consumerID;
   
   // The actual proxy must not get serialized
   protected transient MessageProxy msg;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public AckInfo()
   {      
   }
   
   public AckInfo(MessageProxy proxy, int consumerID)
   {
      this.msg = proxy;
      this.messageID = proxy.getMessage().getMessageID();
      this.consumerID = consumerID;    
   }
   
   public AckInfo(long messageID, int consumerID)
   {
      this.messageID = messageID;
      this.consumerID = consumerID;
   }

   // Public --------------------------------------------------------
   
   public long getMessageID()
   {
      return messageID;
   }
   
   public int getConsumerID()
   {
      return consumerID;
   }
   
   public MessageProxy getMessage()
   {
      return msg;
   }

   public String toString()
   {
      return "AckInfo[" + messageID + ", " + consumerID + "]";
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
     out.writeLong(messageID);
     out.writeInt(consumerID);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      messageID = in.readLong();
      consumerID = in.readInt();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
   
}
