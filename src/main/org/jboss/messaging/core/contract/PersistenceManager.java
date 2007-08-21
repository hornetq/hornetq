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
package org.jboss.messaging.core.contract;

import java.util.List;

import org.jboss.messaging.core.impl.tx.Transaction;

/**
 * The interface to the persistence manager.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:Konda.Madhu@uk.mizuho-sc.com">Madhu Konda</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 * @version <tt>1.1</tt>
 *
 * PersistenceManager.java,v 1.1 2006/02/22 17:33:42 timfox Exp
 */
public interface PersistenceManager extends MessagingComponent
{
	void setPaging(long channelID, boolean paging);

	boolean isPaging();
	
	
   void addReference(long channelID, MessageReference ref, Transaction tx) throws Exception;

   void removeReference(long channelID, MessageReference ref, Transaction tx) throws Exception;
   
   void updateDeliveryCount(long channelID, MessageReference ref) throws Exception;
   
   // XA Recovery functionality
   
   List retrievePreparedTransactions() throws Exception;
   
   List getMessageChannelPairRefsForTx(long transactionId) throws Exception;

   List getMessageChannelPairAcksForTx(long transactionId) throws Exception;

      
   // Paging functionality - TODO we should split this out into its own interface

   void pageReferences(long channelID, List references, boolean paged) throws Exception;
   
   void removeDepagedReferences(long channelID, List refs) throws Exception;
    
   void updatePageOrder(long channelID, List references) throws Exception;
   
   void updateReferencesNotPagedInRange(long channelID, long orderStart, long orderEnd, long num) throws Exception;
   
   List getPagedReferenceInfos(long channelID, long orderStart, int number) throws Exception;
   
   InitialLoadInfo loadFromStart(long channelID, int fullSize) throws Exception;
   
   void mergeTransactions (long fromChannelID, long toChannelID) throws Exception;

   InitialLoadInfo mergeAndLoad(long fromChannelID, long toChannelID, int numberToLoad,
                                long firstPagingOrder, long nextPagingOrder) throws Exception;
     
   List getMessages(List messageIds) throws Exception;
         
   //Counter related functionality - TODO we should split this out into its own interface
   
   long reserveIDBlock(String counterName, int size) throws Exception;
   
   // Failover elated functionality (checkDuplicates on send)

   boolean referenceExists(long messageID) throws Exception;

   // Interface value classes ----------------------------------------------------------------------

   class MessageChannelPair
   {
      private Message message;
      
      private long channelId;
      
      public MessageChannelPair(Message message, long channelId)
      {
         this.message = message;
         
         this.channelId = channelId;
      }
      
      public Message getMessage()
      {
         return message;
      }
      
      public long getChannelId()
      {
         return channelId;
      }
   }
   
   class ReferenceInfo
   {
      private long messageId;
      
      private int deliveryCount;
      
      private long scheduledDelivery;
      
      public ReferenceInfo(long msgId, int deliveryCount, long scheduledDelivery)
      {
         this.messageId = msgId;
         
         this.deliveryCount = deliveryCount;
         
         this.scheduledDelivery = scheduledDelivery;
      }    
      
      public long getMessageId()
      {
         return messageId;
      }
 
      public int getDeliveryCount()
      {
         return deliveryCount;
      }      
      
      public long getScheduledDelivery()
      {
         return scheduledDelivery;
      }
   }
   
   class InitialLoadInfo
   {
      private Long minPageOrdering;
      
      private Long maxPageOrdering;
      
      private List refInfos;

      public InitialLoadInfo(Long minPageOrdering, Long maxPageOrdering, List refInfos)
      {
         this.minPageOrdering = minPageOrdering;
         this.maxPageOrdering = maxPageOrdering;
         this.refInfos = refInfos;
      }

      public Long getMaxPageOrdering()
      {
         return maxPageOrdering;
      }

      public Long getMinPageOrdering()
      {
         return minPageOrdering;
      }
      
      public List getRefInfos()
      {
         return refInfos;
      }
   }
   
}
