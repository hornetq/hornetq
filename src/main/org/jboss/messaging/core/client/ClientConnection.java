/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConnection
{    
   ClientSession createClientSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                                     int ackBatchSize, boolean cacheProducers) throws MessagingException;

   void start() throws MessagingException;

   void stop() throws MessagingException;

   void setFailureListener(FailureListener listener) throws MessagingException;
  
   void close() throws MessagingException;
   
   boolean isClosed();
}
