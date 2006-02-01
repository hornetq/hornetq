/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.server.remoting;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.jboss.logging.Logger;
import org.jboss.remoting.serialization.impl.jboss.JBossSerializationManager;

/**
 * 
 * A JMSSerializationManager.
 * 
 * The only reason we need to override JBossSerializationManager is we need to return
 * JMSObjectInputStream and JMSObjectOutputStream instances since these provide a method
 * to get access to the raw DataOuputStream which we need during custom marshalling
 * to write directly to the stream.
 * 
 * Can get rid of this once current problem with JBossSerialization is resolved
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * JMSSerializationManager.java,v 1.1 2006/02/01 17:38:32 timfox Exp
 */
public class JMSSerializationManager extends JBossSerializationManager
{
   private static final Logger log = Logger.getLogger(JMSSerializationManager.class);
  
   public ObjectInputStream createInput(InputStream input, ClassLoader loader) throws IOException
   {
      if(log.isTraceEnabled())
      {
         log.trace("Creating JMSObjectInputStream");
      }
      return new JMSObjectInputStream(input);
   }

   public ObjectOutputStream createOutput(OutputStream output) throws IOException
   {
      if(log.isTraceEnabled())
      {
         log.trace("Creating JMSObjectOutputStream");
      }
      //return new JMSObjectOutputStream(new CountingOutputStream(output));
      return new JMSObjectOutputStream(output);
   }
  
}
