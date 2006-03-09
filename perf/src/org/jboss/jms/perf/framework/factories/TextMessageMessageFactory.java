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
import javax.jms.TextMessage;

/**
 * 
 * A TextMessageMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class TextMessageMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = -6708553993263367407L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      byte[] bytes = getBytes(size);
      String s = new String(bytes);
      TextMessage theMessage = sess.createTextMessage(s);
      return theMessage;
   }
}
