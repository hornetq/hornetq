/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.util.id.GUID;


public class JBossTemporaryQueue extends JBossQueue implements TemporaryQueue
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 4250425221695034957L;
      
   // Attributes ----------------------------------------------------
   
   private transient ConnectionDelegate delegate;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public JBossTemporaryQueue(ConnectionDelegate delegate)
   {
      super(new GUID().toString());
      this.delegate = delegate;
   }
   
   // Public --------------------------------------------------------
   
   // TemporaryQueue implementation ----------------------------------------------
   
   public void delete() throws JMSException
   {
      if (delegate != null) delegate.deleteTemporaryDestination(this);
   }
   
   // JBossDestination overrides ---------------------------------------------------
   
   public boolean isTemporary()
   {
      return true;
   }
   
   // Package protected ---------------------------------------------
   
   
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
  
}
