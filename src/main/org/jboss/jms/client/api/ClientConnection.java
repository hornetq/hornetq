/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConnection
{
   ClientSession createClientSession(boolean transacted,
                                     int acknowledgmentMode, boolean isXA) throws JMSException;

   String getClientID() throws JMSException;

   int getServerID();
   
   void setClientID(String id) throws JMSException;

   void start() throws JMSException;

   void stop() throws JMSException;

   ExceptionListener getExceptionListener() throws JMSException;
   
   void setExceptionListener(ExceptionListener listener) throws JMSException;
  
   ConnectionMetaData getConnectionMetaData() throws JMSException;
   
   JBossConnectionConsumer createConnectionConsumer(Destination dest,
                                                    String subscriptionName,
                                                    String messageSelector,
                                                    ServerSessionPool sessionPool,
                                                    int maxMessages) throws JMSException;

   MessagingRemotingConnection getRemotingConnection();

   void removeChild(String id) throws JMSException;
   
   void closing() throws JMSException;
   
   void close() throws JMSException;
}
