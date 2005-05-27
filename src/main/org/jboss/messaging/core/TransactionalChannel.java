/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import javax.transaction.TransactionManager;

/**
 * A Channel that guarantees atomic handling of a set of messages or acknowledgments.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface TransactionalChannel extends Channel
{
   /**
    * @return true if the channel was configured to handle messages transactionally, false
    *         otherwise. For the time being, setting a valid transaction manager is sufficient
    *         to enable transactional handling. TODO review this. 
    */
   public boolean isTransactional();

   public void setTransactionManager(TransactionManager tm);

   public TransactionManager getTransactionManager();
}


