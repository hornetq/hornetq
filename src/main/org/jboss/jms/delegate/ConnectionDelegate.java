/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConnectionDelegate
{
   public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode)
          throws JMSException;

   public String getClientID() throws JMSException;
   public void setClientID(String clientID) throws JMSException;

   public void start();
   public void stop();
   public void close();

}
