/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketType
{
   // System
   public static final byte NULL                               = 1;
   public static final byte TEXT                               = 2;
   public static final byte BYTES                              = 3;
   public static final byte PING                               = 4;
   public static final byte PONG                               = 5;

   // Miscellaneous
   public static final byte EXCEPTION                          = 10;
   public static final byte CLOSE                              = 11;

   // Server
   public static final byte CREATECONNECTION                   = 20;
   public static final byte CREATECONNECTION_RESP              = 21;

   // Connection
   public static final byte CONN_CREATESESSION                 = 30;
   public static final byte CONN_CREATESESSION_RESP            = 31;
   public static final byte CONN_START                         = 32;
   public static final byte CONN_STOP                          = 33;

   // Session
   public static final byte SESS_CREATECONSUMER                = 40;
   public static final byte SESS_CREATECONSUMER_RESP           = 41;
   public static final byte SESS_CREATEPRODUCER                = 42;
   public static final byte SESS_CREATEPRODUCER_RESP           = 43;
   public static final byte SESS_CREATEBROWSER                 = 44;
   public static final byte SESS_CREATEBROWSER_RESP            = 45;
   public static final byte SESS_ACKNOWLEDGE                   = 46;
   public static final byte SESS_RECOVER                       = 47;
   public static final byte SESS_COMMIT                        = 48;
   public static final byte SESS_ROLLBACK                      = 49;
   public static final byte SESS_CANCEL                        = 50;
   public static final byte SESS_QUEUEQUERY                    = 51;
   public static final byte SESS_QUEUEQUERY_RESP               = 52;
   public static final byte SESS_CREATEQUEUE                   = 53;
   public static final byte SESS_DELETE_QUEUE                  = 54;
   public static final byte SESS_ADD_DESTINATION               = 55;
   public static final byte SESS_REMOVE_DESTINATION            = 56;
   public static final byte SESS_BINDINGQUERY                  = 57;
   public static final byte SESS_BINDINGQUERY_RESP             = 58;
   public static final byte SESS_BROWSER_RESET                 = 59;
   public static final byte SESS_BROWSER_HASNEXTMESSAGE        = 60;
   public static final byte SESS_BROWSER_HASNEXTMESSAGE_RESP   = 61;
   public static final byte SESS_BROWSER_NEXTMESSAGEBLOCK      = 62;
   public static final byte SESS_BROWSER_NEXTMESSAGEBLOCK_RESP = 63;
   public static final byte SESS_BROWSER_NEXTMESSAGE           = 64;
   public static final byte SESS_BROWSER_NEXTMESSAGE_RESP      = 65;
   public static final byte SESS_XA_START                      = 66;
   public static final byte SESS_XA_END                        = 67;
   public static final byte SESS_XA_COMMIT                     = 68;
   public static final byte SESS_XA_PREPARE                    = 69;
   public static final byte SESS_XA_RESP                       = 70;
   public static final byte SESS_XA_ROLLBACK                   = 71;
   public static final byte SESS_XA_JOIN                       = 72;
   public static final byte SESS_XA_SUSPEND                    = 73;
   public static final byte SESS_XA_RESUME                     = 74;
   public static final byte SESS_XA_FORGET                     = 75;
   public static final byte SESS_XA_INDOUBT_XIDS               = 76;
   public static final byte SESS_XA_INDOUBT_XIDS_RESP          = 77;
   public static final byte SESS_XA_SET_TIMEOUT                = 78;
   public static final byte SESS_XA_SET_TIMEOUT_RESP           = 79;
   public static final byte SESS_XA_GET_TIMEOUT                = 80;
   public static final byte SESS_XA_GET_TIMEOUT_RESP           = 81;

   // Consumer
   public static final byte CONS_FLOWTOKEN                     = 90;
   public static final byte CONS_DELIVER                       = 91;

   // Producer
   public static final byte PROD_SEND                          = 100;
   public static final byte PROD_RECEIVETOKENS                 = 101;
}
