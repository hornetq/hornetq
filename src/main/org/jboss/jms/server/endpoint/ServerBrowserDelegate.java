/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.selector.Selector;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Routable;


/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ServerBrowserDelegate implements BrowserDelegate
{
   private static final Logger log = Logger.getLogger(ServerBrowserDelegate.class);

   private Channel destination;
   private Iterator iterator;
   private String messageSelector;
   private String browserID;
	
   ServerBrowserDelegate(String browserID, Channel destination, String messageSelector)
      throws JMSException
   {     
      this.browserID = browserID;
      this.messageSelector = messageSelector;    
      this.destination = destination;
		Filter filter = null;
		if (messageSelector != null)
		{	
			filter = new Selector(messageSelector);		
		}
		iterator = destination.browse(filter).iterator();
   }
      
   
   public boolean hasNextMessage()
   {
      return iterator.hasNext();
   }
   
   public Message nextMessage()
   {
      return (Message)iterator.next();
   }
   
   
	//Is this the most efficient way to pass it back?
	//why not just pass back the arraylist??
   public Message[] nextMessageBlock(int maxMessages)
   {
      if (maxMessages < 2) throw new IllegalArgumentException("maxMessages must be >=2 otherwise use nextMessage");
      
      ArrayList messages = new ArrayList(maxMessages);
      int i = 0;
      while (i < maxMessages)
      {
         if (iterator.hasNext())
         {
            Message m = (Message)((Routable)iterator.next()).getMessage();
            messages.add(m);
            i++;
         }
         else break;
      }		
		return (Message[])messages.toArray(new Message[messages.size()]);		        
   }
   
   
   public void close() throws JMSException
   {
      iterator = null;
   }
   
   public void closing() throws JMSException
   {
      //Do nothing
   }
}
