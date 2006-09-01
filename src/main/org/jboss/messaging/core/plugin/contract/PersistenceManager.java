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
package org.jboss.messaging.core.plugin.contract;

import java.util.List;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.tx.Transaction;

/**
 * The interface to the persistence manager
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * PersistenceManager.java,v 1.1 2006/02/22 17:33:42 timfox Exp
 */
public interface PersistenceManager extends ServerPlugin
{
   void addReference(long channelID, MessageReference ref, Transaction tx) throws Exception;

   void removeReference(long channelID, MessageReference ref, Transaction tx) throws Exception;
    
   // XA Recovery functionality
   
   List retrievePreparedTransactions() throws Exception;
      
   // Paging functionality - TODO we should split this out into its own interface
   
   void addReferences(long channelID, List references) throws Exception;
   
   void removeReferences(long channelID, List refs) throws Exception;
    
   void updatePageOrder(long channelID, List references) throws Exception;
   
   void updateReliableReferencesNotPagedInRange(long channelID, long orderStart, long orderEnd) throws Exception;
            
   List getPagedReferenceInfos(long channelID, long orderStart, long number) throws Exception;
   
   InitialLoadInfo getInitialReferenceInfos(long channelID, int fullSize) throws Exception;
      
   List getMessages(List messageIds) throws Exception;
         
   //Counter related functionality - TODO we should split this out into its own interface
   
   long reserveIDBlock(String counterName, int size) throws Exception;
   
      
   // Interface value classes
   //---------------------------------------------------------------
   
   class ReferenceInfo
   {
      private long messageId;
      
      private int deliveryCount;
      
      private boolean reliable;
      
      public ReferenceInfo(long msgId, int deliveryCount, boolean reliable)
      {
         this.messageId = msgId;
         
         this.deliveryCount = deliveryCount;
         
         this.reliable = reliable;
      }    
      
      public long getMessageId()
      {
         return messageId;
      }
 
      public int getDeliveryCount()
      {
         return deliveryCount;
      }      
      
      public boolean isReliable()
      {
         return reliable;
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
