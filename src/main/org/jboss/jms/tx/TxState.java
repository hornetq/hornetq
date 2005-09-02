/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tx;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Holds information for a JMS transaction to be sent to the server for
 * processing.
 * Holds the messages to be sent and the acknowledgements to be made
 * for the transaction
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class TxState implements Serializable
{  
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -7255482761072658186L;
   
   public final static byte TX_OPEN = 0;
   public final static byte TX_ENDED = 1;
   public final static byte TX_PREPARED = 3;
   public final static byte TX_COMMITED = 4;
   public final static byte TX_ROLLEDBACK = 5;
   
   // Attributes ----------------------------------------------------
   
   //private Long id;
   public byte state = TX_OPEN;
   public ArrayList messages = new ArrayList();
   public ArrayList acks = new ArrayList();

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   /*
   TxInfo(Long id)
   {
      this.id = id;
   }
   */

   // Public --------------------------------------------------------
   
   /*
   public Long getId()
   {
      return id;
   }
   */
   
   // Externalizable implementation ---------------------------------

   //TODO
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
	

}
