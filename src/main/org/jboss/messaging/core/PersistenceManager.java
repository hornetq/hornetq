/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A PersistenceManager is responsible for managing persistent message state in
 * a persistent store.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface PersistenceManager
{
   void add(Serializable channelID, Delivery d) throws Exception;

   boolean remove(Serializable channelID, Delivery d, Transaction tx)  throws Exception;

   /**
    * @return a List of StorageIdentifiers for all messages for which there are active deliveries.
    */
   List deliveries(Serializable channelID) throws Exception;

   void add(Serializable channelID, MessageReference ref, Transaction tx) throws Exception;

   boolean remove(Serializable channelID, MessageReference ref) throws Exception;

   /**
    * @return a List of StorageIdentifiers for all messages whose delivery hasn't been attempted yet.
    */
   List messages(Serializable channelID) throws Exception;

   void store(Message m) throws Exception;
   
   void remove(String messageID) throws Exception;

   Message retrieve(Serializable messageID) throws Exception;
   
   void removeAllMessageData(Serializable channelID) throws Exception;
   
   void commitTx(Transaction tx) throws Exception;
   
   void rollbackTx(Transaction tx) throws Exception;

}