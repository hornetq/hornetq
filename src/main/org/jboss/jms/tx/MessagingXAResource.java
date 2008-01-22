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
package org.jboss.jms.tx;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.exception.MessagingXAException;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.util.Logger;

/**
 * An XAResource implementation.
 * 
 * This defines the contract for the application server to interact with the resource manager.
 * 
 * It mainly delegates to the resource manager.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * 
 * Parts based on JBoss MQ XAResource implementation by:
 * 
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessagingXAResource implements XAResource
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingXAResource.class);
   
   // Attributes -----------------------------------------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   private ResourceManager rm;
   
   private ClientSession session;
   
   //For testing only
   private boolean preventJoining;

   // Static ---------------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------

   public MessagingXAResource(ResourceManager rm, ClientSession session)
   { 
      this.rm = rm;
      
      this.session = session;
   }
   
   // XAResource implementation --------------------------------------------------------------------

   public boolean setTransactionTimeout(int timeout) throws XAException
   {
      return false;
   }

   public int getTransactionTimeout() throws XAException
   {
      //Default to 10 mins
      //TODO make configurable
      return 600;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException
   {
      if (preventJoining)
      {
         return false;
      }
      
      if (!(xaResource instanceof MessagingXAResource))
      {
         return false;
      }
      
      boolean same = ((MessagingXAResource)xaResource).rm.getServerID() == this.rm.getServerID();
      
      if (trace) { log.trace("Calling isSameRM, result is " + same + " " + ((MessagingXAResource)xaResource).rm.getServerID() + " " + this.rm.getServerID()); }
            
      return same;
   }
   
   public void start(Xid xid, int flags) throws XAException
   {
      if (trace) { log.trace(this + " starting " + xid + ", flags: " + flags); }
      
      // Recreate Xid. See JBMESSAGING-661 [JPL]

      if (!(xid instanceof MessagingXid))
      {
         xid = new MessagingXid(xid);
      }

      boolean convertTx = false;
      
      Object currentXid = session.getCurrentTxId();
      
      // Sanity check
      if (currentXid == null)
      {
         throw new MessagingXAException(XAException.XAER_RMFAIL, "Current xid is not set");
      }
      
      if (flags == TMNOFLAGS && session.getCurrentTxId() instanceof LocalTx)
      {
         convertTx = true;
         
         if (trace) { log.trace("Converting local tx into global tx branch"); }
      }      

      //TODO why do we need this synchronized block?
      synchronized (this)
      {
         switch (flags)
         {
            case TMNOFLAGS :
               if (convertTx)
               {    
                  // If I commit/rollback the tx, then there is a short period of time between the
                  // AS (or whoever) calling commit on the tx and calling start to enrolling the
                  // session in a new tx. If the session has any listeners then in that period,
                  // messages can be received asychronously but we want them to be received in the
                  // context of a tx, so we convert.
                  // Also for an transacted delivery in a MDB we need to do this as discussed
                  // in fallbackToLocalTx()
                  setCurrentTransactionId(rm.convertTx((LocalTx)session.getCurrentTxId(), xid));
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
            default:
               throw new MessagingXAException(XAException.XAER_PROTO, "Invalid flags: " + flags);
         }
      }
   }
   
   public void end(Xid xid, int flags) throws XAException
   {
      if (trace) { log.trace(this + " ending " + xid + ", flags: " + flags); }

      // Recreate Xid. See JBMESSAGING-661 [JPL]

      if (!(xid instanceof MessagingXid))
      {
         xid = new MessagingXid(xid);
      }

      //TODO - why do we need this synchronized block?
      synchronized (this)
      {         
         unsetCurrentTransactionId(xid);    
         
         switch (flags)
         {
            case TMSUSPEND :                                    
               rm.suspendTx(xid);
               break;
            case TMFAIL :
               rm.endTx(xid, false);
               break;
            case TMSUCCESS :
               rm.endTx(xid, true);
               break;
            default :
               throw new MessagingXAException(XAException.XAER_PROTO, "Invalid flags: " + flags);         
         }      
      }
   }
   
   public int prepare(Xid xid) throws XAException
   {
      if (trace) { log.trace(this + " preparing " + xid); }

      // Recreate Xid. See JBMESSAGING-661 [JPL]

      if (!(xid instanceof MessagingXid))
      {
         xid = new MessagingXid(xid);
      }

      return rm.prepare(xid, session.getConnection());
   }
   
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      if (trace) { log.trace(this + " committing " + xid + (onePhase ? " (one phase)" : " (two phase)")); }

      // Recreate Xid. See JBMESSAGING-661 [JPL]

      if (!(xid instanceof MessagingXid))
      {
         xid = new MessagingXid(xid);
      }

      rm.commit(xid, onePhase, session.getConnection());
   }
   
   public void rollback(Xid xid) throws XAException
   {
      if (trace) { log.trace(this + " rolling back " + xid); }

      // Recreate Xid. See JBMESSAGING-661 [JPL]

      if (!(xid instanceof MessagingXid))
      {
         xid = new MessagingXid(xid);
      }

      rm.rollback(xid, session.getConnection());
   }
   
   public void forget(Xid xid) throws XAException
   {
      if (trace) { log.trace(this + " forgetting " + xid + " (currently an NOOP)"); }
   }

   public Xid[] recover(int flags) throws XAException
   {
      if (trace) { log.trace(this + " recovering, flags: " + flags); }

      Xid[] xids = rm.recover(flags, session.getConnection());
      
      if (trace) { log.trace("Recovered txs: " + xids); }
      
      return xids;
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "MessagingXAResource[" + session.getID() + "]";
   }
   
   /*
    * This is used in testing to force isSameRM() to always return false
    * This allows us to test 2PC properly - since otherwise the transaction manager
    * is likely to do a 1PC optimisations if isSameRM() returns true by joining the transaction
    * branches.
    */
   public void setPreventJoining(boolean preventJoining)
   {
      this.preventJoining = preventJoining;
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   private void setCurrentTransactionId(Object xid)
   {
      if (trace) { log.trace(this + " setting current xid to " + xid + ",  previous " + session.getCurrentTxId()); }

      session.setCurrentTxId(xid);
   }
   
   private void unsetCurrentTransactionId(Object xid)
   {
      if (xid == null)
      {
         throw new IllegalArgumentException("xid must be not null");
      }

      if (trace) { log.trace(this + " unsetting current xid " + xid + ",  previous " + session.getCurrentTxId()); }

      // Don't unset the xid if it has previously been suspended.  The session could have been
      // recycled
      if (xid.equals(session.getCurrentTxId()))
      {
         // When a transaction association ends we fall back to acting as if in a local tx
         // This is because for MDBs, the message is received before the global tx
         // has started. Therefore we receive it in a local tx, then convert the work
         // done into the global tx branch when the resource is enlisted.
         // See Mark Little's book "Java Transaction Processing" Chapter 5 for
         // a full explanation
         // So in other words - when the session is not enlisted in a global tx
         // it will always have a local xid set
         
         session.setCurrentTxId(rm.createLocalTx());
      }
   }
   
   // Inner classes --------------------------------------------------------------------------------
}