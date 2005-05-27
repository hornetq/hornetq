/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util.transaction;

import java.util.List;

/**
 * Comment
 *
 * @author <a href="mailto:bill@jboss.org">Bill Burke</a>
 * @version $Revision$
 */
interface RecoveryLogReader
{
   List recover();

   void finishRecovery();

   String getBaseGlobalId();

   String getLogFileName();
}
