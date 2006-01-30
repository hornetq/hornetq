/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import org.jboss.messaging.core.local.ManageableQueue;

/**
 * A deployable JBoss Messaging queue.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Queue extends DestinationServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public Queue()
   {
      super(false);
   }

   public Queue(boolean createProgrammatically)
   {
      super(createProgrammatically);
   }

   // JMX managed attributes ----------------------------------------
   
   public int getCurrentMessageCount() throws Exception
   {
	   ManageableQueue q = (ManageableQueue)dm.getCoreDestination(true, name);
	   return q.getCurrentMessageCount();
   }

   // JMX managed operations ----------------------------------------

   // TODO implement these:

//   int getQueueDepth() throws java.lang.Exception;
//
//   int getScheduledMessageCount() throws java.lang.Exception;
//
//   int getReceiversCount();
//
//   java.util.List listReceivers();
//
//   java.util.List listMessages() throws java.lang.Exception;
//
//   java.util.List listMessages(java.lang.String selector) throws java.lang.Exception;

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
