/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.StreamMessage;

/**
 * 
 * A StreamMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class StreamMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = -5188777779031562013L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      StreamMessage m = sess.createStreamMessage();
      m.writeBytes(getBytes(size));  
      return m;
   }
}
