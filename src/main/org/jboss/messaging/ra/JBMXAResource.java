/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;

/**
 * JBMXAResource.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMXAResource implements XAResource
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMXAResource.class);
   
   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection */
   private JBMManagedConnection managedConnection;
   
   /** The resource */
   private XAResource xaResource;

   /**
    * Create a new JBMXAResource.
    * @param managedConnection the managed connection
    * @param xaResource the xa resource
    */
   public JBMXAResource(JBMManagedConnection managedConnection, XAResource xaResource)
   {
      if (trace)
         log.trace("constructor(" + managedConnection + ", " + xaResource + ")");

      this.managedConnection = managedConnection;
      this.xaResource = xaResource;
   }

   /**
    * Start
    * @param xid A global transaction identifier
    * @param flags One of TMNOFLAGS, TMJOIN, or TMRESUME
    * @exception XAException An error has occurred
    */
   public void start(Xid xid, int flags) throws XAException
   {
      if (trace)
         log.trace("start(" + xid + ", " + flags + ")");

      managedConnection.lock();
      try
      {
         xaResource.start(xid, flags);
      }
      finally
      {
         managedConnection.unlock();
      }
   }

   /**
    * End
    * @param xid A global transaction identifier
    * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND.
    * @exception XAException An error has occurred
    */
   public void end(Xid xid, int flags) throws XAException
   {
      if (trace)
         log.trace("end(" + xid + ", " + flags + ")");

      managedConnection.lock();
      try
      {
         xaResource.end(xid, flags);
      }
      finally
      {
         managedConnection.unlock();
      }
   }

   /**
    * Prepare
    * @param xid A global transaction identifier
    * @return XA_RDONLY or XA_OK
    * @exception XAException An error has occurred
    */
   public int prepare(Xid xid) throws XAException
   {
      if (trace)
         log.trace("prepare(" + xid + ")");

      return xaResource.prepare(xid);
   }

   /**
    * Commit
    * @param xid A global transaction identifier
    * @param onePhase If true, the resource manager should use a one-phase commit protocol to commit the work done on behalf of xid. 
    * @exception XAException An error has occurred
    */
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      if (trace)
         log.trace("commit(" + xid + ", " + onePhase + ")");

      xaResource.commit(xid, onePhase);
   }

   /**
    * Rollback
    * @param xid A global transaction identifier
    * @exception XAException An error has occurred
    */
   public void rollback(Xid xid) throws XAException
   {
      if (trace)
         log.trace("rollback(" + xid + ")");

      xaResource.rollback(xid);
   }

   /**
    * Forget
    * @param xid A global transaction identifier
    * @exception XAException An error has occurred
    */
   public void forget(Xid xid) throws XAException
   {
      if (trace)
         log.trace("forget(" + xid + ")");

      managedConnection.lock();
      try
      {
         xaResource.forget(xid);
      }
      finally
      {
         managedConnection.unlock();
      }
   }

   /**
    * IsSameRM
    * @param xaRes An XAResource object whose resource manager instance is to be compared with the resource manager instance of the target object. 
    * @return True if its the same RM instance; otherwise false. 
    * @exception XAException An error has occurred
    */
   public boolean isSameRM(XAResource xaRes) throws XAException
   {
      if (trace)
         log.trace("isSameRM(" + xaRes + ")");

      return xaResource.isSameRM(xaRes);
   }

   /**
    * Recover 
    * @param flags One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS
    * @return Zero or more XIDs
    * @exception XAException An error has occurred
    */
   public Xid[] recover(int flag) throws XAException
   {
      if (trace)
         log.trace("recover(" + flag + ")");

      return xaResource.recover(flag);
   }

   /**
    * Get the transaction timeout in seconds
    * @return The transaction timeout
    * @exception XAException An error has occurred
    */
   public int getTransactionTimeout() throws XAException
   {
      if (trace)
         log.trace("getTransactionTimeout()");

      return xaResource.getTransactionTimeout();
   }

   /**
    * Set the transaction timeout
    * @param seconds The number of seconds
    * @return True if the transaction timeout value is set successfully; otherwise false. 
    * @exception XAException An error has occurred
    */
   public boolean setTransactionTimeout(int seconds) throws XAException
   {
      if (trace)
         log.trace("setTransactionTimeout(" + seconds + ")");

      return xaResource.setTransactionTimeout(seconds);
   }
}
