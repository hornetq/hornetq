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
package org.jboss.messaging.core.plugin.exchange.cluster;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.tx.TxCallback;

/**
 * A CastMessageCallback
 * 
 * When we want to send persistent message(s) across the group to remote subscriptions
 * and there is at least one remote durable subscription we do the following:
 * 1) Cast the message(s) to group members. They are held in a "holding area" on the remote node
 * and not immediately sent to the channels.
 * 2) Persist the message(s) in the durable subscription(s) on the sending node.
 * 3) Cast another message to the group members telling them to process the messages in the "holding" area
 * for a particular transaction id
 * This allows us to avoid an expensive 2PC protocol which involve extra database updates
 * When the sending node starts up, it records a flag in the database, on clean shutdown it deletes the flag.
 * If the server finds the flag in the database at start-up it knows it crashed the last time, so it
 * sends a "check" message to all members of the group.
 * On receipt of the "check" message, the receiving node checks in it's holding area for any holding messages for
 * that node id, if they exist AND they also exist in the db (i.e. they were persisted ok) then they are processed
 * otherwise they are discarded.
 * 
 * The execution order of callbacks must be as follows:
 * 
 * CastMessagesCallback.beforeCommit() - cast message(s) to holding areas
 * JDBCPersistenceManager.TransactionCallback.beforeCommit() - persist message(s) in database
 * CastMessagesCallback.afterCommit() - send "commit" message to holding areas
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class CastMessagesCallback implements TxCallback
{           
   private List persistent;
   
   private List nonPersistent;
   
   private String nodeId;
   
   private long txId;
   
   private ExchangeInternal exchange;
   
   void addMessage(String routingKey, Message message)
   {
      MessageHolder holder = new MessageHolder(routingKey, message);
      
      if (message.isReliable())
      {
         if (persistent == null)
         {
            persistent = new ArrayList();
         }
         persistent.add(holder);
      }
      else
      {
         if (nonPersistent == null)
         {
            nonPersistent = new ArrayList();
         }
         nonPersistent.add(holder);
      }
   }
   
   CastMessagesCallback(String nodeId, long txId, ExchangeInternal exchange)
   {
      this.nodeId = nodeId;
      
      this.txId = txId;
      
      this.exchange = exchange;
   }

   public void afterCommit(boolean onePhase) throws Exception
   {            
      if (nonPersistent != null)
      {
         // Cast the non persistent - this don't need to go into a holding area on the receiving node
         ExchangeRequest req = new MessagesRequest(nonPersistent);
         
         exchange.asyncSendRequest(req);
      }
      
      if (persistent != null)
      {
         // Cast a commit message
         ExchangeRequest req = new TransactionRequest(nodeId, txId);
         
         // Stack must be FIFO
         exchange.asyncSendRequest(req);
      }
      
      nonPersistent = persistent = null;
   }

   public void afterPrepare() throws Exception
   { 
   }

   public void afterRollback(boolean onePhase) throws Exception
   {
   }

   public void beforeCommit(boolean onePhase) throws Exception
   {
      if (persistent != null)
      {
         //We send the persistent messages which go into the "holding area" on
         //the receiving nodes
         ExchangeRequest req = new TransactionRequest(nodeId, txId, persistent);
         
         //Stack must be FIFO
         exchange.asyncSendRequest(req);
      }
   }

   public void beforePrepare() throws Exception
   {
   }

   public void beforeRollback(boolean onePhase) throws Exception
   {
   }
      
}    
