/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;


import org.jboss.messaging.jms.client.BrowserDelegate;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

/**
 * A browser.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class JBossBrowser implements QueueBrowser
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The delegate */
   private BrowserDelegate delegate;
   
   /** The queue */
   private Queue queue;
   
   /** The message selector */
   private String selector;

	// Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Create a new JBossBrowser.
    * 
    * @param delegate the delegate.
    * @param queue the queue.
    * @param selector the selector.
    */
   public JBossBrowser(BrowserDelegate delegate, Queue queue, String selector)
   {
      this.delegate = delegate;
      this.queue = queue;
      this.selector = selector;
   }

   // QueueBrowser implementation -----------------------------------

   public void close() throws JMSException
   {
      delegate.closing();
      delegate.close();
   }

   public Enumeration getEnumeration() throws JMSException
   {
      return new BrowserEnumeration(delegate.browse());
   }

   public String getMessageSelector() throws JMSException
   {
      return selector;
   }

   public Queue getQueue() throws JMSException
   {
      return queue;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
