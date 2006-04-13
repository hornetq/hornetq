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
import org.jboss.jms.selector.Selector;

/**
 * A deployable JBoss Messaging queue.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
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
      org.jboss.messaging.core.local.Queue q = (org.jboss.messaging.core.local.Queue)cm.getCoreDestination(jbq);
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
      org.jboss.messaging.core.local.Queue q = (org.jboss.messaging.core.local.Queue)cm.getCoreDestination(jbq);
      q.removeAllMessages();
   }
   
   public List listMessages(String selector) throws JMSException
   {
      if (!started)
      {
         log.warn("Queue is stopped.");
         return new ArrayList();
      }
      
      if (selector != null)
      {
         selector = selector.trim();
         if (selector.equals(""))
         {
            selector = null;
         }
      }

      JBossQueue jbq = new JBossQueue(name);
      org.jboss.messaging.core.local.Queue q = (org.jboss.messaging.core.local.Queue)cm.getCoreDestination(jbq);
      try 
      {
         List msgs;
         if (selector == null)
         {
            msgs = q.browse();
         }
         else
         {
            msgs = q.browse(new Selector(selector));
         }
         return msgs;
      }
      catch (InvalidSelectorException e)
      {
         Throwable th = new JMSException(e.getMessage());
         th.initCause(e);
         throw (JMSException)th;
      }
   }
    
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
