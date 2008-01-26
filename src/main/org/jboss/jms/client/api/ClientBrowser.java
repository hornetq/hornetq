/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;

import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientBrowser
{
   void reset() throws JMSException;

   Message nextMessage() throws JMSException;
   
   boolean hasNextMessage() throws JMSException;
      
   Message[] nextMessageBlock(int maxMessages) throws JMSException;
   
   void closing() throws JMSException;
   
   void close() throws JMSException;
 }
