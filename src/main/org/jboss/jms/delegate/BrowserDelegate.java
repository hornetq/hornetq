/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Message;

import org.jboss.jms.client.Closeable;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public interface BrowserDelegate extends Closeable
{
   
   //void reset();
      
   Message nextMessage();
   
   boolean hasNextMessage();
      
   Message[] nextMessageBlock(int maxMessages);
   
}
   

