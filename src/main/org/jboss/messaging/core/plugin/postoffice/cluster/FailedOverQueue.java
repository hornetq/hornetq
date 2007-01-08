/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.plugin.postoffice.cluster;

import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.Filter;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 *
 * A LocalClusteredQueue
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class FailedOverQueue extends LocalClusteredQueue
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int failedNodeID;

   // Constructors ---------------------------------------------------------------------------------

   public FailedOverQueue(PostOffice office, int nodeID, String name, long id, MessageStore ms,
                          PersistenceManager pm, boolean acceptReliableMessages,
                          boolean recoverable, QueuedExecutor executor, Filter filter,
                          TransactionRepository tr, int fullSize, int pageSize, int downCacheSize,
                          int failedNodeID)
   {
      super(office, nodeID, name, id, ms, pm, acceptReliableMessages, recoverable,
            executor, filter, tr, fullSize, pageSize, downCacheSize);

      this.failedNodeID = failedNodeID;
   }

   public FailedOverQueue(PostOffice office, int nodeID, String name, long id, MessageStore ms,
                          PersistenceManager pm, boolean acceptReliableMessages,
                          boolean recoverable, QueuedExecutor executor, Filter filter,
                          TransactionRepository tr, int failedNodeID)
   {
      super(office, nodeID, name, id, ms, pm, acceptReliableMessages, recoverable,
            executor, filter, tr);

      this.failedNodeID = failedNodeID;
   }

   // Public ---------------------------------------------------------------------------------------

   public int getFailedNodeID()
   {
      return failedNodeID;
   }

   public String toString()
   {
      return "FailedOverQueue[" + getChannelID() + "/" + getName() +
         "/failedNodeID=" + failedNodeID + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
