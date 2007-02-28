/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.server.endpoint;

import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.plugin.IDBlock;
import org.jboss.messaging.core.tx.MessagingXid;


/**
 * Represents the set of methods from the ConnectionDelegate that are handled on the server. The
 * rest of the methods are handled in the advice stack.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ConnectionEndpoint extends Closeable
{
   SessionDelegate createSessionDelegate(boolean transacted,
                                         int acknowledgmentMode,
                                         boolean isXA) throws JMSException;

   String getClientID() throws JMSException;

   void setClientID(String id) throws JMSException;

   void start() throws JMSException;

   void stop() throws JMSException;

   void sendTransaction(TransactionRequest request, boolean checkForDuplicates) throws JMSException;

   MessagingXid[] getPreparedTransactions() throws JMSException; 
   
   IDBlock getIdBlock(int size) throws JMSException;
}

