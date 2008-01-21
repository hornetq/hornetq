/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientBrowser extends Closeable
{
   /**
    * Reset the internal state of the browser endpoint so the following
    * nextMessage()/hasNextMessage()/nextMessageBlock() invocations would reflect the state of the
    * queue at the moment of the reset.
    */
   void reset() throws JMSException;

   Message nextMessage() throws JMSException;
   
   boolean hasNextMessage() throws JMSException;
      
   Message[] nextMessageBlock(int maxMessages) throws JMSException;
 }
