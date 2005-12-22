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
package org.jboss.jms.client;

import org.jboss.jms.server.Version;

import java.util.Enumeration;
import java.util.Vector;
import java.io.Serializable;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

/**
 * Connection metadata
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class JBossConnectionMetaData implements Serializable, ConnectionMetaData
{

   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 327633302671160939L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Version serverVersion;

   // Constructors --------------------------------------------------

   /**
    * Create a new JBossConnectionMetaData object.
    */
   public JBossConnectionMetaData(Version serverVersion)
   {
      this.serverVersion = serverVersion;
   }

   // ConnectionMetaData implementation -----------------------------

   public String getJMSVersion() throws JMSException
   {
      return serverVersion.getJMSVersion();
   }

   public int getJMSMajorVersion() throws JMSException
   {
      return serverVersion.getJMSMajorVersion();
   }

   public int getJMSMinorVersion() throws JMSException
   {
      return serverVersion.getJMSMinorVersion();
   }

   public String getJMSProviderName() throws JMSException
   {
      return serverVersion.getJMSProviderName();
   }

   public String getProviderVersion() throws JMSException
   {
      return serverVersion.getProviderVersion();
   }

   public int getProviderMajorVersion() throws JMSException
   {
      return serverVersion.getProviderMajorVersion();
   }

   public int getProviderMinorVersion() throws JMSException
   {
      return serverVersion.getProviderMinorVersion();
   }

   public Enumeration getJMSXPropertyNames() throws JMSException
   {
      Vector v = new Vector();
      v.add("JMSXGroupID");
      v.add("JMSXGroupSeq");
      return v.elements();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
