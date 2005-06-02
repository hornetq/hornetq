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
 * 
 * 
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class TxInfo implements Serializable
{  
	private static final long serialVersionUID = -7255482761072658186L;

   private Long id;
   private ArrayList messages = new ArrayList();
   private ArrayList acks = new ArrayList();
	
   TxInfo(Long id)
   {
      this.id = id;
   }
   
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
	
	//TODO - Custom serialization
}
