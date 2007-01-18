/*
* JBoss, Home of Professional Open Source
* Copyright 2006, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.recovery;

import java.util.Hashtable;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.logging.Logger;

/**
 * BridgeXAResourceWrapper.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 1.2 $
 */
public class BridgeXAResourceWrapper implements XAResource, ExceptionListener
{
   /** The log */
   private static final Logger log = Logger.getLogger(XAResourceWrapper.class);
   
   private boolean trace = log.isTraceEnabled();

   /** The state lock */
   private static final Object lock = new Object();
   
   /** The connection */
   private XAConnection connection;
   
   /** The delegate XAResource */
   private XAResource delegate;
   
   private Hashtable jndiProperties;
   
   private String connectionFactoryLookup;
   
   public BridgeXAResourceWrapper(Hashtable jndiProperties, String connectionFactoryLookup)
   {
      this.jndiProperties = jndiProperties;
      
      this.connectionFactoryLookup = connectionFactoryLookup;
   }
   
   public Xid[] recover(int flag) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling recover"); }
                  
      try
      {
         return xaResource.recover(flag);
      }
      catch (XAException e)
      {
         log.info("Caught exception in recover", e);
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in recover", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling commit"); }
      
      try
      {
         xaResource.commit(xid, onePhase);
      }
      catch (XAException e)
      {
         log.info("Caught exception in commit", e);
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in commit", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void rollback(Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling rollback"); }
      
      try
      {
         xaResource.rollback(xid);
      }
      catch (XAException e)
      {
         log.info("Caught exception in rollback", e);
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in rollback", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void forget(Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling forget"); }
      
      
      try
      {
         xaResource.forget(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in forget", e);
         throw new RuntimeException(e.toString());
      }
   }

   public boolean isSameRM(XAResource xaRes) throws XAException
   {
      if (xaRes instanceof XAResourceWrapper)
         xaRes = ((XAResourceWrapper) xaRes).getDelegate();
      
      if (trace) { log.trace(this + " Calling isSameRM"); }
      

      XAResource xaResource = getDelegate();
      try
      {
         return xaResource.isSameRM(xaRes);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in issamerm", e);
         throw new RuntimeException(e.toString());
      }
   }

   public int prepare(Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling prepare"); }
      
      
      try
      {
         return xaResource.prepare(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in prepare", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void start(Xid xid, int flags) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling start"); }
      
      try
      {
         xaResource.start(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in start", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void end(Xid xid, int flags) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling end"); }
      
      try
      {
         xaResource.end(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in end", e);
         throw new RuntimeException(e.toString());
      }
   }

   public int getTransactionTimeout() throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling getTransactionTimeout"); }
      
      try
      {
         return xaResource.getTransactionTimeout();
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in getTransactiontimeoiut", e);
         throw new RuntimeException(e.toString());
      }
   }

   public boolean setTransactionTimeout(int seconds) throws XAException
   {
      XAResource xaResource = getDelegate();
      
      if (trace) { log.trace(this + " Calling setTransactionTimeout"); }
      try
      {
         return xaResource.setTransactionTimeout(seconds);
      }
      catch (XAException e)
      {
         throw check(e);
      }
      catch (Exception e)
      {
         log.info("Caught e in settranactiotntimeoiut", e);
         throw new RuntimeException(e.toString());
      }
   }

   public void onException(JMSException exception)
   {
      log.warn("Notified of connection failure in recovery delegate", exception);
      close();
   }
   
   /**
    * Get the delegate XAResource
    * 
    * @return the delegate
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
         error = e;
      }

      if (result == null)
      {
         XAException xae = new XAException("Error trying to connect");
         xae.errorCode = XAException.XAER_RMERR;
         if (error != null)
            xae.initCause(error);
         log.debug("Cannot get delegate XAResource", xae);
         throw xae;
      }
      
      return result;
   }
   
   /**
    * Connect to the server if not already done so
    * 
    * @return the delegate XAResource
    * @throws Exception for any problem
    */
   protected XAResource connect() throws Exception
   {
      // Do we already have a valid delegate?
      synchronized (lock)
      {
         if (delegate != null)
            return delegate;
      }
      
      if (trace) { log.trace(this + " Connecting"); }
      
      
      // Create the connection
      XAConnection xaConnection = getConnectionFactory().createXAConnection();
      synchronized (lock)
      {
         connection = xaConnection;
      }

      // Retrieve the delegate XAResource
      try
      {
         XASession session = connection.createXASession();
         XAResource result = session.getXAResource();
         synchronized (lock)
         {
            delegate = result;
         }
         return delegate;
      }
      catch (Exception e)
      {
         close();
         throw e;
      }
   }

   /**
    * Get the XAConnectionFactory
    * 
    * @return the connection
    * @throws Exception for any problem
    */
   protected XAConnectionFactory getConnectionFactory() throws Exception
   {
      InitialContext ic = null;
      
      try
      {
         ic = new InitialContext(jndiProperties);
         
         XAConnectionFactory connectionFactory = (XAConnectionFactory)ic.lookup(connectionFactoryLookup);
         
         return connectionFactory;
      }
      finally
      {
         if (ic != null)
         {
            ic.close();
         }
      }
   }
   
   /**
    * Close the connection
    */
   public void close()
   {
      if (trace) { log.trace(this + " Close"); }
      
      try
      {
         XAConnection oldConnection = null;
         synchronized (lock)
         {
            oldConnection = connection;
            connection = null;
            delegate = null;
         }
         if (oldConnection != null)
            oldConnection.close();
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
      if (trace) { log.trace(this + " check " + e); }
      
      if (e.errorCode == XAException.XAER_RMERR || e.errorCode == XAException.XAER_RMFAIL)
      {
         log.debug("Fatal error", e);
         close();
      }
      throw e;
   }

   protected void finalize() throws Throwable
   {
      close();
   }
}
