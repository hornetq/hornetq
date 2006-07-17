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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * Holds information for a JMS transaction to be sent to the server for
 * processing.
 * Holds the messages to be sent and the acknowledgements to be made
 * for the transaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox </a>
 */
public class TxState implements Externalizable
{  
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -7255482761072658186L;
   
   public final static byte TX_OPEN = 0;
   
   public final static byte TX_ENDED = 1;
   
   public final static byte TX_PREPARED = 2;
   
   public final static byte TX_COMMITED = 3;
   
   public final static byte TX_ROLLEDBACK = 4;
   
   // Attributes ----------------------------------------------------
   
   protected int state = TX_OPEN;
   
   protected List messages = new ArrayList();
   
   protected List acks = new ArrayList();
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TxState()
   {      
   }
   
   // Public --------------------------------------------------------
   
   public int getState()
   {
      return state;
   }
   
   public List getMessages()
   {
      return messages;
   }
   
   public List getAcks()
   {
      return acks;
   }
   
   public void clearMessages()
   {
      messages.clear();
   }
   
   public void setState(int state)
   {
      this.state = state;
   }
    
   // Externalizable implementation ---------------------------------
   
   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeInt(state);
      if (messages == null)
      {
         out.writeInt(-1);
      }
      else
      {
         out.writeInt(messages.size());
         Iterator iter = messages.iterator();
         while (iter.hasNext())
         {
            JBossMessage m = (JBossMessage)iter.next();
            //We don't use writeObject to avoid serialization overhead
            out.writeByte(m.getType());
            m.writeExternal(out);
         } 
      }
      if (acks == null)
      {
         out.writeInt(-1);
      }
      else
      {
         out.writeInt(acks.size());
         Iterator iter = acks.iterator();
         while (iter.hasNext())
         {
            AckInfo a = (AckInfo)iter.next();
            //We don't use writeObject to avoid serialization overhead
            a.writeExternal(out);
         }
      }
   }
   
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      state = in.readInt();
      int numMessages = in.readInt();
      if (numMessages == -1)
      {
         messages = null;
      }
      else
      {
         messages = new ArrayList(numMessages);
         for (int i = 0; i < numMessages; i++)
         {
            byte type = in.readByte();
            JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);
            m.readExternal(in);
            messages.add(m);
         }
      }
      
      int numAcks = in.readInt();
      if (numAcks == -1)
      {
         acks = null;
      }
      else
      {
         acks = new ArrayList(numAcks);
         for (int i = 0; i < numAcks; i++)
         {
            AckInfo info = new AckInfo();
            info.readExternal(in);
            acks.add(info);
         }
      }  
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------
   
   // Private -------------------------------------------------------
      
   // Inner Classes -------------------------------------------------
      
}
