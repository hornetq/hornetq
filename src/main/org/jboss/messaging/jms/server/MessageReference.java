/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server;

import org.jboss.messaging.jms.message.JBossMessage;

/**
 * A message reference
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageReference
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   JBossMessage getMessage() throws Exception;
   
   String getMessageID() throws Exception;

   int getPriority() throws Exception;
}
