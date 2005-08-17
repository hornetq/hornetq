/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.util.id.GUID;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossTemporaryTopic extends JBossTopic implements TemporaryTopic
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -1412919224718697967L;
      
   // Attributes ----------------------------------------------------
   
   private transient SessionDelegate delegate;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public JBossTemporaryTopic(SessionDelegate delegate)
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
