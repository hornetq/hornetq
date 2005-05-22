/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.QueueBrowser;
import javax.jms.Queue;
import javax.jms.JMSException;

import java.util.Enumeration;

import org.jboss.jms.delegate.BrowserDelegate;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class JBossQueueBrowser implements QueueBrowser
{
   private BrowserDelegate delegate;
   private Queue queue;
   private String messageSelector; 
   
   private BrowserEnumeration enumeration = new BrowserEnumeration();
   
   
   JBossQueueBrowser(Queue queue, String messageSelector, BrowserDelegate delegate)
   {
      this.delegate = delegate;
      this.queue = queue;
      this.messageSelector = messageSelector;
   }
   
   public void close() throws JMSException
   {
      delegate.closing();
	   delegate.close();
   }
 
   public Enumeration getEnumeration() throws JMSException
   {               
      return enumeration;
   }
  
   public String getMessageSelector() throws JMSException
   {
      return messageSelector;
   }

   public Queue getQueue() throws JMSException
   {
      return queue;
   }
         
   private class BrowserEnumeration implements Enumeration
   {            
      public boolean hasMoreElements()
      {
         return delegate.hasNextMessage();
      }
     
      public Object nextElement()
      {
         return delegate.nextMessage();
      }
   }
   
}
