/*
 * Created on Mar 29, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jboss.jms.client;

import java.util.Enumeration;
import java.util.Vector;
import java.io.Serializable;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

/**
 * Connection metadata
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 *
 * $Id$
 */
public class JBossConnectionMetaData implements Serializable, ConnectionMetaData {
   
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
      
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   /**
    * Create a new JBossConnectionMetaData object.
    */
   public JBossConnectionMetaData() {}
   
   
   // ConnectionMetaData Implementation
   
   public String getJMSVersion() throws JMSException {
      return "1.1";
   }
   
   
   public int getJMSMajorVersion() throws JMSException {
      return 1;
   }
   
   
   public int getJMSMinorVersion() throws JMSException {
      return 1;
   }
   
   
   public String getJMSProviderName() throws JMSException {
      return "JBoss Messaging";
   }
   
   
   public String getProviderVersion() throws JMSException {        
      return "1.0.0 alpha PR1";
   }
   
   
   public int getProviderMajorVersion() throws JMSException {
      return 1;
   }
   
   
   public int getProviderMinorVersion() throws JMSException {
      return 0;
   }
   
   
   public Enumeration getJMSXPropertyNames() throws JMSException {
      Vector v = new Vector();
      v.add("JMSXGroupID");
      v.add("JMSXGroupSeq");
      return v.elements();
   }
   
}
