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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.DefaultAck;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.impl.message.MessageFactory;

/**
 * Holds the state of a transaction on the client side
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox </a>
 */
public class ClientTransaction
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientTransaction.class);

   public final static byte TX_OPEN = 0;
   public final static byte TX_ENDED = 1;
   public final static byte TX_PREPARED = 2;
   public final static byte TX_COMMITED = 3;
   public final static byte TX_ROLLEDBACK = 4;

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private byte state = TX_OPEN;

   // Map<Integer(sessionID) - SessionTxState> maintained on the client side
   private Map sessionStatesMap;

   // Read from on the server side
   private List sessionStatesList;

   private boolean clientSide;
   
   private boolean hasPersistentAcks;
   
   private boolean failedOver;
   
   private boolean removeAcks;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientTransaction()
   {
      clientSide = true;
   }

   // Public --------------------------------------------------------

   public byte getState()
   {
      return state;
   }

   public void addMessage(String sessionId, JBossMessage msg)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      SessionTxState sessionTxState = getSessionTxState(sessionId);

      sessionTxState.addMessage(msg);
   }
   
   public void addAck(String sessionId, DeliveryInfo info)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      SessionTxState sessionTxState = getSessionTxState(sessionId);

      sessionTxState.addAck(info);
      
      if (info.getMessageProxy().getMessage().isReliable())
      {
         hasPersistentAcks = true;
      }
      
      if (!info.isShouldAck())
      {
      	removeAcks = true;
      }
   }
   
   public boolean hasPersistentAcks()
   {
      return hasPersistentAcks;
   }
   
   public boolean isFailedOver()
   {
      return failedOver;
   }
   
   public void clearMessages()
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }

      if (sessionStatesMap != null)
      {
         // This can be null if the tx was recreated on the client side due to recovery

         for(Iterator i = sessionStatesMap.values().iterator(); i.hasNext(); )
         {
            SessionTxState sessionTxState = (SessionTxState)i.next();
            sessionTxState.clearMessages();
         }
      }
   }

   public void setState(byte state)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      this.state = state;
   }

   public List getSessionStates()
   {
      if (sessionStatesList != null)
      {
         return sessionStatesList;
      }
      else
      {
         return sessionStatesMap == null ?
            Collections.EMPTY_LIST : new ArrayList(sessionStatesMap.values());
      }
   }

   /*
   * Substitute newSessionID for oldSessionID
   */
   public void handleFailover(int newServerID, String oldSessionID, String newSessionID)
   {
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }
      
      // Note we have to do this in one go since there may be overlap between old and new session
      // IDs and we don't want to overwrite keys in the map.

      Map tmpMap = null;

      if (sessionStatesMap != null)
      {
         for(Iterator i = sessionStatesMap.values().iterator(); i.hasNext();)
         {
            SessionTxState state = (SessionTxState)i.next();
            
            boolean handled = state.handleFailover(newServerID, oldSessionID, newSessionID);

            if (handled)
            {
	            if (tmpMap == null)
	            {
	               tmpMap = new LinkedHashMap();
	            }
	            tmpMap.put(newSessionID, state);
            }
         }
      }

      if (tmpMap != null)
      {
         // swap
         sessionStatesMap = tmpMap;
      }
      
      failedOver = true;
   }

   /**
    * May return an empty list, but never null.
    */
   public List getDeliveriesForSession(String sessionID)
   {   	   	
      if (!clientSide)
      {
         throw new IllegalStateException("Cannot call this method on the server side");
      }

      if (sessionStatesMap == null)
      {
         return Collections.EMPTY_LIST;
      }
      else
      {         
         SessionTxState state = (SessionTxState)sessionStatesMap.get(sessionID);
   
         if (state != null)
         {
            List list = state.getAcks();
            
            return list;
         }
         else
         {
            return Collections.EMPTY_LIST;
         }
      }            
   }

   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      out.writeByte(state);

      if (sessionStatesMap == null)
      {
         out.writeInt(0);
      }
      else
      {
         out.writeInt(sessionStatesMap.size());

         Iterator iter = sessionStatesMap.values().iterator();

         while (iter.hasNext())
         {
            SessionTxState state = (SessionTxState)iter.next();

            out.writeUTF(state.getSessionId());

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

            iter2 = acks.iterator();

            while (iter2.hasNext())
            {
               DeliveryInfo ack = (DeliveryInfo)iter2.next();
               
               //We don't want to send acks for things like non durable subs which will have been already acked
               if (ack.isShouldAck())
               {
               	//We only need the delivery id written
               	out.writeLong(ack.getMessageProxy().getDeliveryId());
               }
            }
            
            //Marker for end of acks
            out.writeLong(Long.MIN_VALUE);
         }
      }
   }


   public void read(DataInputStream in) throws Exception
   {
      clientSide = false;

      state = in.readByte();

      int numSessions = in.readInt();
      
      //Read in as a list since we don't want the extra overhead of putting into a map
      //which won't be used on the server side
      sessionStatesList = new ArrayList(numSessions);

      for (int i = 0; i < numSessions; i++)
      {
         String sessionId = in.readUTF();

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

         long l;
         
         while ((l = in.readLong()) != Long.MIN_VALUE)
         {
         	sessionState.addAck(new DefaultAck(l));
         }
      }
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   private SessionTxState getSessionTxState(String sessionID)
   {
      if (sessionStatesMap == null)
      {
         sessionStatesMap = new LinkedHashMap();
      }

      SessionTxState sessionTxState = (SessionTxState)sessionStatesMap.get(sessionID);

      if (sessionTxState == null)
      {
         sessionTxState = new SessionTxState(sessionID);
         
         sessionStatesMap.put(sessionID, sessionTxState);
      }

      return sessionTxState;
   }

   // Inner Classes -------------------------------------------------

   public class SessionTxState
   {
      private String sessionID;

      // We record the server id when doing failover to avoid overwriting the sesion ID again if
      // multiple connections fail on the same resource mamanger but fail onto old values of the
      // session ID. This prevents the ID being failed over more than once for the same server.
      private int serverID = -1;

      private List msgs = new ArrayList();
      private List acks = new ArrayList();

      SessionTxState(String sessionID)
      {
         this.sessionID = sessionID;
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

      public String getSessionId()
      {
         return sessionID;
      }
      
      public void setAcks(List acks)
      {
      	this.acks = acks;
      }

      boolean handleFailover(int newServerID, String oldSessionID, String newSessionID)
      {
         if (sessionID.equals(oldSessionID) && serverID != newServerID)
         {
            sessionID = newSessionID;
            serverID = newServerID;

            // Remove any non persistent acks
            for(Iterator i = acks.iterator(); i.hasNext(); )
            {
               DeliveryInfo di = (DeliveryInfo)i.next();

               if (!di.getMessageProxy().getMessage().isReliable())
               {
                  if (trace) { log.trace(this + " discarded non-persistent " + di + " on failover"); }
                  i.remove();
               }
            }
            return true;
         }
         else
         {
         	return false;
         }
      }

      void clearMessages()
      {
         msgs.clear();
      }

   }

}
