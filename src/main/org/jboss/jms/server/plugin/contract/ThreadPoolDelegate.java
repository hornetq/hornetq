/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin.contract;

import org.jboss.messaging.core.plugin.contract.ServerPlugin;

/**
 * A thread pool contract. The pool implementation controls how threads are allocated to deliver
 * messages to consumers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ThreadPoolDelegate extends ServerPlugin
{
   void execute(Runnable runnable) throws InterruptedException;
}
