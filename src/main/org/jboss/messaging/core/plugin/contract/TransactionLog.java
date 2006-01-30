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

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.MessageReference;

/**
 * The main interface to the transactional log.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface TransactionLog extends ServerPlugin
{
   //
   // Used by the TransactionRepository
   //

   void prepareTx(Transaction tx) throws Exception;

   void commitTx(Transaction tx) throws Exception;

   void rollbackTx(Transaction tx) throws Exception;

   List retrievePreparedTransactions() throws Exception;

   //
   // Used by Channel's RecoverableState
   //

   void addReference(Serializable channelID, MessageReference ref, Transaction tx) throws Exception;

   void removeReference(Serializable channelID, MessageReference ref, Transaction tx) throws Exception;

   /**
    * @return a List of StorageIdentifiers for all messages whose delivery hasn't been attempted yet.
    */
   List messageRefs(Serializable storeID, Serializable channelID) throws Exception;

   //
   // Used by ServerSessionEndpoint
   //

   /**
    * TODO Do we really need this method?
    */
   void removeAllMessageData(Serializable channelID) throws Exception;

}
