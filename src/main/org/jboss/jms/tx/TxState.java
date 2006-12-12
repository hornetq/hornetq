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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.util.Streamable;

/**
 * Holds information for a JMS transaction to be sent to the server for
 * processing.
 * Holds the messages to be sent and the acknowledgements to be made
 * for the transaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox </a>
 */
public class TxState implements Streamable
{  
   // Constants -----------------------------------------------------

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


   /** Navigate on ACK and change consumer ids on every ACK not sent yet */
   public void handleFailover(int oldConsumerID, int newConsumerID)
   {
       for (Iterator ackIterator = acks.iterator(); ackIterator.hasNext(); )
       {
           AckInfo ackInfo = (AckInfo)ackIterator.next();
           
           if (ackInfo.getConsumerID() == oldConsumerID)
           {
               ackInfo.setConsumerID(newConsumerID);
           }
       }
   }
   
   public void getAckInfosForConsumerIds(List ackInfos, Set consumerIds)
   {
      for (Iterator ackIterator = acks.iterator(); ackIterator.hasNext(); )
      {
          AckInfo ackInfo = (AckInfo)ackIterator.next();
          
          if (consumerIds.contains(new Integer(ackInfo.getConsumerID())))
          {
              ackInfos.add(ackInfo);
          }
      }
   }
   
   public void removeNonPersistentAcks(Set consumerIds)
   {
      for (Iterator ackIterator = acks.iterator(); ackIterator.hasNext(); )
      {
          AckInfo ackInfo = (AckInfo)ackIterator.next();
          
          if (!ackInfo.msg.getMessage().isReliable())
          {
             ackIterator.remove();
          }
      }
   }
    
   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
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
            m.write(out);
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
            a.write(out);
         }
      }
   }
   
   public void read(DataInputStream in) throws Exception
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
            m.read(in);
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
            info.read(in);
            acks.add(info);
         }
      }  
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------
   
   // Private -------------------------------------------------------
      
   // Inner Classes -------------------------------------------------
      
}
