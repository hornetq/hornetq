/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.List;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossQueue;
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
   
   public int getMessageCount() throws JMSException
   {
      if (!started)
      {
         log.warn("Queue is stopped.");
         return 0;
      }

      JBossQueue jbq = new JBossQueue(name);
	   ManageableQueue q = (ManageableQueue)cm.getCoreDestination(jbq);
	   return q.getMessageCount();
   }

   // JMX managed operations ----------------------------------------
   
   public void removeAllMessages() throws JMSException
   {
      if (!started)
      {
         log.warn("Queue is stopped.");
         return;
      }

      JBossQueue jbq = new JBossQueue(name);
      ManageableQueue q = (ManageableQueue)cm.getCoreDestination(jbq);
      q.removeAllMessages();
   }
   
   public List listMessages(String selector) throws JMSException
   {
      if (!started)
      {
         log.warn("Queue is stopped.");
         return new ArrayList();
      }

      JBossQueue jbq = new JBossQueue(name);
      ManageableQueue q = (ManageableQueue)cm.getCoreDestination(jbq);
      try 
      {
         return q.getMessages(trimSelector(selector));
      }
      catch (InvalidSelectorException e)
      {
         Throwable th = new JMSException(e.getMessage());
         th.initCause(e);
         throw (JMSException)th;
      }
   }

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
