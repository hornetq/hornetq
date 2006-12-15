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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.endpoint.Ack;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * Holds the state of a transaction on the client side
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox </a>
 */
public class ClientTransaction
{  
   // Constants -----------------------------------------------------

   public final static byte TX_OPEN = 0;
   
   public final static byte TX_ENDED = 1;
   
   public final static byte TX_PREPARED = 2;
   
   public final static byte TX_COMMITED = 3;
   
   public final static byte TX_ROLLEDBACK = 4;
   
   // Attributes ----------------------------------------------------
   
   private int state = TX_OPEN;
   
   //Maintained on the client side
   private Map sessionStatesMap;
   
   //Read from on the server side
   private List sessionStatesList;
   
   private boolean clientSide;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public ClientTransaction()
   {                  
      clientSide = true;
   }
   
   // Public --------------------------------------------------------
   
   public int getState()
   {
      return state;
   }
   
   public void addMessage(int sessionId, JBossMessage msg)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      SessionTxState sessionTxState = getSessionTxState(sessionId);
      
      sessionTxState.addMessage(msg);
   }
   
   public void addAck(int sessionId, DeliveryInfo info)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      SessionTxState sessionTxState = getSessionTxState(sessionId);
      
      sessionTxState.addAck(info);
   }      
   
   public void clearMessages()
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      Iterator iter = sessionStatesMap.values().iterator();
      
      while (iter.hasNext())
      {
         SessionTxState sessionTxState = (SessionTxState)iter.next();
         
         sessionTxState.clearMessages();
      }
   }
   
   public void setState(int state)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      this.state = state;
   }
   
   public Collection getSessionStates()
   {
      if (sessionStatesList != null)
      {
         return sessionStatesList;
      }
      else
      {
         return sessionStatesMap == null ? Collections.emptySet() : sessionStatesMap.values();
      }
   }   
   
   /*
    * Substitute newSessionId for oldSessionId
    */
   public void handleFailover(Map oldNewSessionMap)
   {    
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      //Note we have to do this in one go since there may be overlap between old and new session ids
      //and we don't want to overwrite keys in the map
      
      if (sessionStatesMap != null)
      {         
         Map newMap = new HashMap();
         
         Iterator iter = oldNewSessionMap.entrySet().iterator();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            Integer oldSessionId = (Integer)entry.getKey();
            
            SessionState newSessionState = (SessionState)entry.getValue();
            
            int newSessionId = newSessionState.getSessionId();
            
            SessionTxState state = (SessionTxState)sessionStatesMap.get(oldSessionId);
            
            if (state != null)
            {
               state.handleFailover(newSessionId);
            }
            
            newMap.put(new Integer(newSessionId), state);
            
         }
       
         sessionStatesMap = newMap;
      }
   }
   
   public List getDeliveriesForSession(int sessionId)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      SessionTxState state = getSessionTxState(sessionId);
      
      if (state != null)
      {
         return state.getAcks();
      }
      else
      {
         return null;
      }
   }
   
   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(state);
      
      out.writeInt(sessionStatesMap.size());
      
      Iterator iter = sessionStatesMap.values().iterator();
      
      while (iter.hasNext())
      {
         SessionTxState state = (SessionTxState)iter.next();
         
         out.writeInt(state.getSessionId());
         
         List msgs = state.getMsgs();
         
         out.writeInt(msgs.size());
         
         Iterator iter2 = msgs.iterator();
         
         while (iter2.hasNext())
         {
            JBossMessage m = (JBossMessage)iter2.next();

            out.writeByte(m.getType());
            
            m.write(out);
         }
         
         List acks = state.getAcks();
         
         out.writeInt(acks.size());
         
         iter2 = acks.iterator();
         
         while (iter2.hasNext())
         {
            DeliveryInfo ack = (DeliveryInfo)iter2.next();

            //We only need the delivery id written
            out.writeLong(ack.getMessageProxy().getDeliveryId());
         }
      }
   }  
    
   
   public void read(DataInputStream in) throws Exception
   {
      clientSide = false;
      
      state = in.readInt();
      
      int numSessions = in.readInt();
      
      //Read in as a list since we don't want the extra overhead of putting into a map
      //which won't be used on the server side
      sessionStatesList = new ArrayList(numSessions);
      
      for (int i = 0; i < numSessions; i++)
      {
         int sessionId = in.readInt();
         
         SessionTxState sessionState = new SessionTxState(sessionId);
         
         sessionStatesList.add(sessionState);
         
         int numMsgs = in.readInt();
         
         for (int j = 0; j < numMsgs; j++)            
         {
            byte type = in.readByte();
            
            JBossMessage msg = (JBossMessage)MessageFactory.createMessage(type);
            
            msg.read(in);
            
            sessionState.addMessage(msg);
         }
         
         int numAcks = in.readInt();
         
         for (int j = 0; j < numAcks; j++)            
         {
            long ack = in.readLong();
            
            sessionState.addAck(new DefaultAck(ack));
         }
      }      
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------
   
   // Private -------------------------------------------------------
   
   private SessionTxState getSessionTxState(int sessionId)
   {                  
      if (sessionStatesMap == null)
      {
         sessionStatesMap = new HashMap();
      }
      
      SessionTxState sessionTxState = (SessionTxState)sessionStatesMap.get(new Integer(sessionId));
      
      if (sessionTxState == null)
      {
         sessionTxState = new SessionTxState(sessionId);
         
         sessionStatesMap.put(new Integer(sessionId), sessionTxState);
      }
      
      return sessionTxState;
   }
   
   
      
   // Inner Classes -------------------------------------------------
   
   public class SessionTxState
   {
      SessionTxState(int sessionId)
      {
         this.sessionId = sessionId;
      }
      
      void addMessage(JBossMessage msg)
      {
         msgs.add(msg);
      }
      
      void addAck(Ack ack)
      {
         acks.add(ack);
      }
      
      public List getMsgs()
      {
         return msgs;
      }
      
      public List getAcks()
      {
         return acks;
      }
      
      public int getSessionId()
      {
         return sessionId;
      }
      
      void handleFailover(int newSessionId)
      {
         this.sessionId = newSessionId;
         
         //Remove any non persistent acks
         
         Iterator iter = acks.iterator();
         
         while (iter.hasNext())
         {
            DeliveryInfo info = (DeliveryInfo)iter.next();
            
            if (!info.getMessageProxy().getMessage().isReliable())
            {
               iter.remove();
            }
         }
      }
      
      void clearMessages()
      {
         msgs.clear();
      }
      
      private int sessionId;
      
      private List msgs = new ArrayList();
      
      private List acks = new ArrayList();            
   }
      
}
