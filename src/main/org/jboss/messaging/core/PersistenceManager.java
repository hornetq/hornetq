/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import javax.transaction.SystemException;
import java.io.Serializable;
import java.util.List;

/**
 * A reliable, transactional (JTA) PersistenceManager.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface PersistenceManager
{
   void add(Serializable channelID, Delivery d) throws Throwable;

   boolean remove(Serializable channelID, Delivery d)  throws Throwable;

   /**
    * @return a List of StorageIdentifiers for all messages for which there are active deliveries.
    */
   List deliveries(Serializable channelID) throws Throwable;

   void add(Serializable channelID, MessageReference ref) throws Throwable;

   boolean remove(Serializable channelID, MessageReference ref) throws Throwable;

   /**
    * @return a List of StorageIdentifiers for all messages whose delivery hasn't been attempted yet.
    */
   List messages(Serializable channelID) throws Throwable;

   /**
    * Transactional handling only. Needs a JTA transaction.
    *
    * @exception SystemException - thrown when rollback cannot be completed.
    */
   void add(Serializable channelID, Serializable txID, MessageReference r) throws SystemException;

   /**
    * Needs a JTA transaction.
    *
    * @exception SystemException - thrown when rollback cannot be completed.
    */
   void enableTransactedMessages(Serializable channelID, Serializable txID) throws SystemException;

   /**
    * Needs a JTA transaction.
    *
    * @exception SystemException - thrown when rollback cannot be completed.
    */
   void dropTransactedMessages(Serializable channelID, Serializable txID) throws SystemException;


   void store(Message m) throws Throwable;
   
   void remove(String messageID) throws Throwable;

   /**
    * @return null if no such message is found.
    *
    * @exception Throwable - problem with the persistent storage.
    */
   Message retrieve(Serializable messageID) throws Throwable;
   
   void removeAllMessageData(Serializable channelID) throws Exception;

}