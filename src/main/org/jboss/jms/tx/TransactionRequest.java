/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */
package org.jboss.jms.tx;

import java.io.Serializable;

import javax.transaction.xa.Xid;

/**
 * This class contians all the data needed to perform a JMS transaction
 * 
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author Daniel Bloomfield Ramagem (daniel.ramagem@gmail.com) 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class TransactionRequest implements Serializable
{
   // Constants -----------------------------------------------------
   
   /** The serialVersionUID */
   private static final long serialVersionUID = -5371388526898322431L;
   
   /** One phase Commit request */
   public final static byte ONE_PHASE_COMMIT_REQUEST = 0;
   /** Two phase Prepare phase */
   public final static byte TWO_PHASE_COMMIT_PREPARE_REQUEST = 1;
   /** Two phase Commit phase */
   public final static byte TWO_PHASE_COMMIT_COMMIT_REQUEST = 2;
   /** Rollback request */
   public final static byte TWO_PHASE_COMMIT_ROLLBACK_REQUEST = 3;
   
   // Attributes ----------------------------------------------------
   
   /** Request type */
   public byte requestType = ONE_PHASE_COMMIT_REQUEST;

   /** For 2 phase commit, this identifies the transaction. */
   public Xid xid;

   public TxState txInfo;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Externalizable implementation ---------------------------------

   //TODO
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}