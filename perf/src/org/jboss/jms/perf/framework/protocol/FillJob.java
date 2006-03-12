/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.perf.framework.factories.MessageFactory;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Context;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>

 * @version $Revision$
 *
 * $Id$
 */
public class FillJob extends JobSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 339586193389055268L;
   private static final Logger log = Logger.getLogger(FillJob.class);
   public static final String TYPE = "FILL";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected int numMessages;  // TODO - it's already availabe in JobSupport
   protected int deliveryMode;  // TODO - it's already availabe in JobSupport
   protected int msgSize; // TODO - it's already availabe in JobSupport
   protected MessageFactory mf;  // TODO - it's already availabe in JobSupport


   // Constructors --------------------------------------------------

   // Job implemenation ---------------------------------------------

   public String getType()
   {
      return TYPE;
   }

   // JobSupport overrides ---------------------------------------------

   protected Result doWork(Context context) throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(destination);
         prod.setDeliveryMode(deliveryMode);

         for (int i = 0; i < numMessages; i++)
         {
            Message m = mf.getMessage(sess, msgSize);
            prod.send(m);
         }

         log.debug("filled");

         return null;
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Public --------------------------------------------------------

   public void setDeliveryMode(int deliveryMode)
   {
      this.deliveryMode = deliveryMode;
   }

   public String toString()
   {
      return "FILL JOB";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}