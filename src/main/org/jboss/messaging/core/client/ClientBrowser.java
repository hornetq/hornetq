/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.server.MessagingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientBrowser
{
   void reset() throws MessagingException;

   Message nextMessage() throws MessagingException;
   
   boolean hasNextMessage() throws MessagingException;
      
   Message[] nextMessageBlock(int maxMessages) throws MessagingException;
   
   void close() throws MessagingException;
   
   boolean isClosed();
 }
