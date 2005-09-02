/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */

package org.jboss.jms.client;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;
import org.jboss.logging.Logger;

/**
 * An XAResource for the Session - delegates most of it's work to the resource manager
 * 
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class JBossXAResource implements XAResource
{
   // Constants -----------------------------------------------------

   /** The log */
   private static final Logger log = Logger.getLogger(JBossXAResource.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();
   
   // Attributes ----------------------------------------------------

   private ResourceManager rm;
   //private SessionState session;
   
   private Object currentTxID;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   /**
    * Create a new SpyXAResource
    *
    * @param session the session
    */
   public JBossXAResource(ResourceManager rm)
   {
      trace = log.isTraceEnabled();
      
      this.rm = rm;
      //this.session = session;
   }
   
   // Public --------------------------------------------------------
   
   public Object getCurrentTxID()
   {
      return currentTxID;
   }
   
   public void setCurrentTxID(Object currentTxID)
   {
      this.currentTxID = currentTxID;
   }
   
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
         return false;
      return ((JBossXAResource)xaResource).rm == this.rm;
   }
   
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      if (trace)
         log.trace("Commit xid=" + xid + ", onePhase=" + onePhase + " " + this);

      rm.commit(xid, onePhase);
   }

   public void end(Xid xid, int flags) throws XAException
   {
      if (trace)
         log.trace("End xid=" + xid + ", flags=" + flags + " " +this);

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
      if (trace)
         log.trace("Forget xid=" + xid + " " + this);
   }

   public int prepare(Xid xid) throws XAException
   {
      if (log.isTraceEnabled())
         log.trace("Prepare xid=" + xid + " " + this);

      return rm.prepare(xid);
   }

   public Xid[] recover(int arg1) throws XAException
   {
      if (log.isTraceEnabled())
         log.trace("Recover arg1=" + arg1 + " " + this);

      return new Xid[0];
   }

   public void rollback(Xid xid) throws XAException
   {
      if (log.isTraceEnabled())
         log.trace("Rollback xid=" + xid + " " + this);

      rm.rollback(xid);

   }

   public void start(Xid xid, int flags) throws XAException
   {
      if (log.isTraceEnabled())
         log.trace("Start xid=" + xid + ", flags=" + flags + " " + this);

      boolean convertTx = false;
      
      if (this.currentTxID != null)
      {
         if (flags == TMNOFLAGS && this.currentTxID instanceof LocalTxXid)
         {
            convertTx = true;
         }
         else
         {
            //This resource is already doing work as part of another global tx
            throw new XAException(XAException.XAER_OUTSIDE);
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
                  // convert it over to a normal XID tansaction.
                  
                  //Is it legal to "convert" a tx?
                  //Surely only work done between start and end is considered part of the tx?
                  //Is it legal to consider work done before "start" as part of the tx?
                                    
                  setCurrentTransactionId(rm.convertTx((LocalTxXid)this.currentTxID, xid));
               }
               else
               {                  
                  setCurrentTransactionId(rm.startTx(xid));
                  if (log.isTraceEnabled()) { log.trace("Setting current tx on session state as:" + xid); }
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

      if (trace)
         log.trace("Setting current tx xid=" + xid + " previous: " + this.currentTxID + " " + this);

      this.currentTxID = xid;
   }
   
   private void unsetCurrentTransactionId(final Xid xid)
   {
      if (xid == null)
         throw new org.jboss.util.NullArgumentException("xid");

      if (trace)
         log.trace("Unsetting current tx  xid=" + xid + " previous: " + this.currentTxID + " " + this);

      // Don't unset the xid if it has previously been suspended
      // The session could have been recycled
      if (xid.equals(this.currentTxID))
         this.currentTxID = null;
   }
   
   // Inner classes -------------------------------------------------
}