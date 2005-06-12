/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tx;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Message;

/**
 * Holds information for a JMS transaction to be sent to the server for
 * processing.
 * Holds the messages to be sent and the acknowledgements to be made
 * for the transaction
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class TxInfo implements Serializable
{  
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -7255482761072658186L;
   
   // Attributes ----------------------------------------------------
   
   private Long id;
   private ArrayList messages = new ArrayList();
   private ArrayList acks = new ArrayList();

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   TxInfo(Long id)
   {
      this.id = id;
   }

   // Public --------------------------------------------------------
   
   public Long getId()
   {
      return id;
   }
   
   public void addMessage(Message mess)
   {      
      messages.add(mess);
   }
   
   public void addAck(AckInfo ack)
   {
      acks.add(ack);
   }
   
   public List getMessages()
   {
      return messages;
   }
   
   public List getAcks()
   {
      return acks;
   }

   // Externalizable implementation ---------------------------------

   //TODO
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
	

}
