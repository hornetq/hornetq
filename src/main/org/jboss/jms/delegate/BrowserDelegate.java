/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.client.Closeable;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public interface BrowserDelegate extends Closeable
{
   
   Message nextMessage() throws JMSException;
   
   boolean hasNextMessage() throws JMSException;
      
   Message[] nextMessageBlock(int maxMessages) throws JMSException;
   
}
   

