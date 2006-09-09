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

/**
 * A Transactionid
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class TransactionId
{
   private String nodeId;
   
   private long txId;
   
   private int hash;
   
   TransactionId(String nodeId, long txId)
   {
      this.nodeId = nodeId;
      
      this.txId = txId;
      
      calculateHash();
   }
   
   String getNodeId()
   {
      return nodeId;
   }
   
   long getTxId()
   {
      return txId;
   }
   
   public int hashCode()
   {
      return hash;
   }
   
   public boolean equals(Object other)
   {
      if (other == this)
      {
         return true;
      }
      
      if (!(other instanceof TransactionId))
      {
         return false;
      }
      
      TransactionId tother = (TransactionId)other;
      
      return tother.txId == this.txId && tother.nodeId.equals(this.nodeId);
   }
   
   public String toString()
   {
      return "TransactionId [" + System.identityHashCode(this) + "] nodeId: " + nodeId + " txId: " + txId;
   }
   
   private void calculateHash()
   {
      hash = 17;
      
      hash = 37 * hash + (int)(txId ^ (txId >>> 32));
      
      hash = 37 * hash + nodeId.hashCode();
   }
}
