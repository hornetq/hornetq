/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util.transaction;

import javax.transaction.xa.Xid;

/**
 * Comment
 *
 * @author <a href="mailto:bill@jboss.org">Bill Burke</a>
 * @version $Revision$
 */
interface RecoveryLogger
{
   public RecoveryLogReader[] getRecoveryLogs();

   public RecoveryLogTerminator committing(Xid xid);

}
