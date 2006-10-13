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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.tx.TransactionException;
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
 * Failure handling:
 * 
 * If failure of the remote node occurs after casting the message to the holding area
 * but before the message is persisted locally, then no recovery is necessary since the message wasn't persisted.
 * In this case an exception should be propagated to the client.
 * 
 * If failure of the remote node occurs after casting the message to the holding area
 * and after the message has been persisted locally then, when the failover node takes
 * over it will load the message from the db.
 * 
 * If failure of the local node occurs before casting the message, no recovery is necessary and an exception
 * should be thrown to the client.
 * 
 * If failure of the local node occurs after casting the message, but before persisting the message.
 * Then failure of the local node casues the remote node to check it's holding area - it will then check
 * if the message has been persisted - in this case it has not so it will discard the tx from the holding area.
 * (TODO is it possible that the cast of the original hold arrives *after* the receiving node knows the sending
 * node has failed - we must be able to guarantee this never happens)
 * 
 * If failure of the local node occurs after casting the message and after persisting the message in the database.
 * Then failure of the local node casues the remote node to check it's holding area - it will then check
 * if the message has been persisted - in this case it has so it will excute the transaction in memory which will add the
 * message to the in memory queue.
 * 
 * 
 * 
 * 
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class CastMessagesCallback implements TxCallback
{           
   private static final Logger log = Logger.getLogger(CastMessagesCallback.class);
   
   private boolean trace = log.isTraceEnabled();   
      
   private List persistent;
   
   private List nonPersistent;
   
   private int nodeId;
   
   private long txId;
   
   private PostOfficeInternal office;
   
   private boolean multicast;
   
   private int toNodeId;
   
   //Used in failure testing
   private boolean failBeforeCommit;
   private boolean failAfterCommit;
      
   /*
    * We store the id of one of the channels that the ref was inserted into
    * this is used after node failure to determine whether the tx has to be committed
    * or rolled back on the remote node
    */
   private long checkChannelID;
   
   void addMessage(String routingKey, Message message, Map queueNameToNodeIdMap,
                   int lastNodeId, long channelID)
   {
      //If we only ever send messages to the same node for this tx, then we can unicast rather than multicast
      //This is how we determine that
      if (lastNodeId == -1)
      {
         multicast = true;
      }
      else
      {
         if (lastNodeId != toNodeId)
         {
            multicast = true;
         }
         else
         {
            toNodeId = lastNodeId;
         }
      }
      
      MessageHolder holder = new MessageHolder(routingKey, message, queueNameToNodeIdMap);
      
      if (message.isReliable())
      {
         if (persistent == null)
         {
            persistent = new ArrayList();
         }
         persistent.add(holder);
         
         checkChannelID = channelID;
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
   
   CastMessagesCallback(int nodeId, long txId, PostOfficeInternal office, boolean failBeforeCommit, boolean failAfterCommit)
   {
      this.nodeId = nodeId;
      
      this.txId = txId;
      
      this.office = office;
      
      this.failBeforeCommit = failBeforeCommit;
      
      this.failAfterCommit = failAfterCommit;
   }

   public void afterCommit(boolean onePhase) throws Exception
   {            
      if (nonPersistent != null)
      {
         // Cast the non persistent - this don't need to go into a holding area on the receiving node
         ClusterRequest req = new MessagesRequest(nonPersistent);
         
         sendRequest(req);         
      }
      
      if (persistent != null)
      {
         //Only used in testing
         if (failAfterCommit)
         {
            throw new TransactionException("Forced failure for testing");
         }
         
         // Cast a commit message
         ClusterRequest req = new SendTransactionRequest(nodeId, txId);
         
         sendRequest(req);
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
         ClusterRequest req = new SendTransactionRequest(nodeId, txId, persistent, checkChannelID);
         
         sendRequest(req);
      }
      
      //Only used in testing
      if (failBeforeCommit)
      {
         throw new TransactionException("Forced failure for testing");
      }
   }

   public void beforePrepare() throws Exception
   {
   }

   public void beforeRollback(boolean onePhase) throws Exception
   {
   }
   
   private void sendRequest(ClusterRequest req) throws Exception
   {
      if (multicast)
      {
         if (trace) { log.trace("Multicasting transaction across group"); }
         office.asyncSendRequest(req);
      }
      else
      {
         if (trace) { log.trace("Unicasting transaction to node"); }
         office.asyncSendRequest(req, toNodeId);
      }
   }
      
}    
