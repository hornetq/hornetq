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
package org.jboss.jms.client;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;
import org.jboss.logging.Logger;

/**
 * An XAResource implementation.
 * 
 * This defines the contract for the application server to interact with the resource manager.
 * 
 * It basically just delegates to the resource manager.
 * 
 * TODO This functionality in this class should be put in the resource manager class
 * and the resource manager should implement XAResource
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Parts based on JBoss MQ XAResource implementation by:
 * 
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossXAResource implements XAResource
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossXAResource.class);
   
   // Attributes ----------------------------------------------------

   private ResourceManager rm;
   
   private SessionState sessionState;
   
   private ConnectionDelegate connection;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JBossXAResource(ResourceManager rm, SessionState sessionState)
   { 
      this.rm = rm;
      
      this.sessionState = sessionState;   
      
      this.connection = (ConnectionDelegate)((ConnectionState)sessionState.getParent()).getDelegate();
   }
   
   // Public --------------------------------------------------------
   
 
   // XAResource implementation -------------------------------------

   public boolean setTransactionTimeout(int timeout) throws XAException
   {
      return false;
   }

   public int getTransactionTimeout() throws XAException
   {
      return 0;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException
   {
      if (!(xaResource instanceof JBossXAResource))
      {
         return false;
      }
      
      return ((JBossXAResource)xaResource).rm == this.rm;
   }
   
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Commit xid=" + xid + ", onePhase=" + onePhase + " " + this);
      }

      rm.commit(xid, onePhase, connection);
   }

   public void end(Xid xid, int flags) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("End xid=" + xid + ", flags=" + flags + " " +this);
      }

      synchronized (this)
      {
         switch (flags)
         {
            case TMSUSPEND :
               unsetCurrentTransactionId(xid);
               rm.suspendTx(xid);
               break;
            case TMFAIL :
               unsetCurrentTransactionId(xid);
               rm.endTx(xid, false);
               break;
            case TMSUCCESS :
               unsetCurrentTransactionId(xid);
               rm.endTx(xid, true);
               break;
         }
      }
   }
   
   public void forget(Xid xid) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Forget xid=" + xid + " " + this);
      }
   }

   public int prepare(Xid xid) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Prepare xid=" + xid + " " + this);
      }

      return rm.prepare(xid, connection);
   }

   public Xid[] recover(int arg1) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Recover arg1=" + arg1 + " " + this);
      }

      return new Xid[0];
   }

   public void rollback(Xid xid) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Rollback xid=" + xid + " " + this);
      }

      rm.rollback(xid, connection);
   }

   public void start(Xid xid, int flags) throws XAException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Start xid=" + xid + ", flags=" + flags + " " + this);
      }

      boolean convertTx = false;
      
      if (sessionState.getCurrentTxId() != null)
      {
         if (flags == TMNOFLAGS && sessionState.getCurrentTxId() instanceof LocalTxXid)
         {
            convertTx = true;
         }
      }

      synchronized (this)
      {

         switch (flags)
         {
            case TMNOFLAGS :
               if (convertTx)
               {
                  // it was an anonymous TX, TM is now taking control over it.
                  // convert it over to a normal XID.
                  
                  //Is it legal to "convert" a tx?
                  //Surely only work done between start and end is considered part of the tx?
                  //Is it legal to consider work done before "start" as part of the tx?
                                    
                  setCurrentTransactionId(rm.convertTx((LocalTxXid)sessionState.getCurrentTxId(), xid));
               }
               else
               {                  
                  setCurrentTransactionId(rm.startTx(xid));                 
               }
               break;
            case TMJOIN :
               setCurrentTransactionId(rm.joinTx(xid));
               break;
            case TMRESUME :
               setCurrentTransactionId(rm.resumeTx(xid));
               break;
         }
      }
   }
   
   
   // Object overrides ----------------------------------------------   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void setCurrentTransactionId(final Xid xid)
   {
      if (xid == null)
         throw new org.jboss.util.NullArgumentException("xid");

      if (log.isTraceEnabled())
      {
         log.trace("Setting current tx xid=" + xid + " previous: " + sessionState.getCurrentTxId() + " " + this);
      }
      sessionState.setCurrentTxId(xid);
   }
   
   private void unsetCurrentTransactionId(final Xid xid)
   {
      if (xid == null)
         throw new org.jboss.util.NullArgumentException("xid");

      if (log.isTraceEnabled())
      {
         log.trace("Unsetting current tx  xid=" + xid + " previous: " + sessionState.getCurrentTxId() + " " + this);
      }
      
      // Don't unset the xid if it has previously been suspended
      // The session could have been recycled
      if (xid.equals(sessionState.getCurrentTxId()))
      {
         sessionState.setCurrentTxId(rm.createLocalTx());
      }

   }
   
   // Inner classes -------------------------------------------------
}