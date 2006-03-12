/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.SimpleResult;
import org.jboss.jms.perf.framework.remoting.Context;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class PingJob extends JobSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -23312534535892666L;

   private transient static final Logger log = Logger.getLogger(PingJob.class);

   public static final String TYPE = "PING";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int sleep = 2;

   // Constructors --------------------------------------------------

   public PingJob()
   {
   }

   // JobSupport overrides ------------------------------------------

   protected Result doWork(Context context) throws Exception
   {
      log.info("sleeping for " + sleep + " seconds");
      Thread.sleep(sleep * 1000);
      log.info("woke up, returning");
      return new SimpleResult();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "PING";
   }

   public String getType()
   {
      return TYPE;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
