/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.recovery;

import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;

/**
 * XAResourceWrapper.
 * 
 * Mainly from org.jboss.server.XAResourceWrapper from the JBoss AS server module
 * 
 * The reason why we don't use that class directly is that it assumes on failure of connection
 * the RM_FAIL or RM_ERR is thrown, but in JBM we throw XA_RETRY since we want the recovery manager to be able
 * to retry on failure without having to manually retry
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox/a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version $Revision: 45341 $
 */
public class MessagingXAResourceWrapper implements XAResource, FailureListener
{
   /** The log */
   private static final Logger log = Logger.getLogger(MessagingXAResourceWrapper.class);

   /** The state lock */
   private static final Object lock = new Object();

   /** The JNDI lookup for the XA connection factory */
   private final String connectorFactoryClassName;

   private Map<String, Object> connectorConfig;

   private final String username;

   private final String password;

   private ClientSessionFactory csf;

   private XAResource delegate;

   public MessagingXAResourceWrapper(final String connectorFactoryClassName,
                                     final Map<String, Object> connectorConfig,
                                     final String username, 
                                     final String password)
   {
      this.connectorFactoryClassName = connectorFactoryClassName;
      this.connectorConfig = connectorConfig;
      this.username = username;
      this.password = password;
   }

   public Xid[] recover(int flag) throws XAException
   {
      log.debug("Recover " + connectorFactoryClassName);
      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.recover(flag);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      log.debug("Commit " + connectorFactoryClassName + " xid " + " onePhase=" + onePhase);
      XAResource xaResource = getDelegate();
      try
      {
         xaResource.commit(xid, onePhase);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void rollback(Xid xid) throws XAException
   {
      log.debug("Rollback " + connectorFactoryClassName + " xid ");
      XAResource xaResource = getDelegate();
      try
      {
         xaResource.rollback(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void forget(Xid xid) throws XAException
   {
      log.debug("Forget " + connectorFactoryClassName + " xid ");
      XAResource xaResource = getDelegate();
      try
      {
         xaResource.forget(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public boolean isSameRM(XAResource xaRes) throws XAException
   {
      if (xaRes instanceof MessagingXAResourceWrapper)
         xaRes = ((MessagingXAResourceWrapper)xaRes).getDelegate();

      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.isSameRM(xaRes);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public int prepare(Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.prepare(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void start(Xid xid, int flags) throws XAException
   {
      XAResource xaResource = getDelegate();
      try
      {
         xaResource.start(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void end(Xid xid, int flags) throws XAException
   {
      XAResource xaResource = getDelegate();
      try
      {
         xaResource.end(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public int getTransactionTimeout() throws XAException
   {
      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.getTransactionTimeout();
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public boolean setTransactionTimeout(int seconds) throws XAException
   {
      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.setTransactionTimeout(seconds);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public boolean connectionFailed(MessagingException me)
   {
      log.warn("Notified of connection failure in recovery connectionFactory for provider " + connectorFactoryClassName, me);
      close();
      return true;
   }

   /**
    * Get the connectionFactory XAResource
    * 
    * @return the connectionFactory
    * @throws XAException for any problem
    */
   public XAResource getDelegate() throws XAException
   {
      XAResource result = null;
      Exception error = null;
      try
      {
         result = connect();
      }
      catch (Exception e)
      {
         log.error("********************************Failed to connect to server", e);
         error = e;
      }

      if (result == null)
      {
         XAException xae = new XAException("Error trying to connect to provider " + connectorFactoryClassName);
         xae.errorCode = XAException.XAER_RMERR;
         if (error != null)
            xae.initCause(error);
         log.debug("Cannot get connectionFactory XAResource", xae);
         throw xae;
      }

      return result;
   }

   /**
    * Connect to the server if not already done so
    * 
    * @return the connectionFactory XAResource
    * @throws Exception for any problem
    */
   protected XAResource connect() throws Exception
   {
      // Do we already have a valid connectionFactory?
      synchronized (lock)
      {
         if (delegate != null)
            return delegate;
      }

      TransportConfiguration config = new TransportConfiguration(connectorFactoryClassName, connectorConfig);
      csf = new ClientSessionFactoryImpl(config);
      ClientSession cs = null;

      if (username == null)
      {
         cs = csf.createSession(true, false, false);
      }
      else
      {
         cs = csf.createSession(username, password, true, false, false, false, 1);
      }
      cs.addFailureListener(this);

      synchronized (lock)
      {
         delegate = cs;
      }
      
      return delegate;
   }

   /**
    * Close the connection
    */
   public void close()
   {
      try
      {
         ClientSessionFactory oldCSF = null;
         synchronized (lock)
         {
            oldCSF = csf;
            csf = null;
            delegate = null;
         }
         if (oldCSF != null)
            oldCSF.close();
      }
      catch (Exception ignored)
      {
         log.trace("Ignored error during close", ignored);
      }
   }

   /**
    * Check whether an XAException is fatal. If it is an RM problem
    * we close the connection so the next call will reconnect.
    * 
    * @param e the xa exception
    * @return never
    * @throws XAException always
    */
   protected XAException check(XAException e) throws XAException
   {
      if (e.errorCode == XAException.XA_RETRY)
      {
         log.debug("Fatal error in provider " + connectorFactoryClassName, e);
         close();
      }
      throw new XAException(XAException.XAER_RMFAIL);
   }

   protected void finalize() throws Throwable
   {
      close();
   }
}
