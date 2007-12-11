/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public enum PacketType
{
   NULL                           ((byte) 1),
   MSG_JMSEXCEPTION               ((byte) 2),
   TEXT                           ((byte) 3),

   // Connection factory
   REQ_CREATECONNECTION           ((byte)10),
   RESP_CREATECONNECTION          ((byte)11),
   REQ_GETCLIENTAOPSTACK          ((byte)12),
   RESP_GETCLIENTAOPSTACK         ((byte)13),
   REQ_GETTOPOLOGY                ((byte)14),
   RESP_GETTOPOLOGY               ((byte)15), 
   MSG_UPDATECALLBACK             ((byte)16),
   
   // Connection
   REQ_IDBLOCK                    ((byte)20),
   RESP_IDBLOCK                   ((byte)21),
   REQ_CREATESESSION              ((byte)22),
   RESP_CREATESESSION             ((byte)23),
   MSG_STARTCONNECTION            ((byte)24),
   MSG_STOPCONNECTION             ((byte)25),
   MSG_SENDTRANSACTION            ((byte)26),
   RESP_SENDTRANSACTION           ((byte)27),
   REQ_GETPREPAREDTRANSACTIONS    ((byte)28),
   RESP_GETPREPAREDTRANSACTIONS   ((byte)29),
   REQ_GETCLIENTID                ((byte)30),
   RESP_GETCLIENTID               ((byte)31),
   MSG_SETCLIENTID                ((byte)32),
   
   // Session
   REQ_CREATECONSUMER             ((byte)40),
   RESP_CREATECONSUMER            ((byte)41),  
   REQ_CREATEDESTINATION          ((byte)42),
   RESP_CREATEDESTINATION         ((byte)43),  
   MSG_ADDTEMPORARYDESTINATION    ((byte)44),
   MSG_DELETETEMPORARYDESTINATION ((byte)45),
   REQ_CREATEBROWSER              ((byte)46),
   RESP_CREATEBROWSER             ((byte)47),
   MSG_SENDMESSAGE                ((byte)48),
   MSG_DELIVERMESSAGE             ((byte)49),
   REQ_ACKDELIVERY                ((byte)50),
   RESP_ACKDELIVERY               ((byte)51),
   MSG_ACKDELIVERIES              ((byte)52),
   RESP_ACKDELIVERIES             ((byte)53),
   MSG_RECOVERDELIVERIES          ((byte)54),
   MSG_CANCELDELIVERY             ((byte)55),
   MSG_CANCELDELIVERIES           ((byte)56),
   MSG_UNSUBSCRIBE                ((byte)57),
   
   // Consumer 
   MSG_CHANGERATE                 ((byte)70),
   
   // Browser
   MSG_BROWSER_RESET              ((byte)80),
   REQ_BROWSER_HASNEXTMESSAGE     ((byte)81),
   RESP_BROWSER_HASNEXTMESSAGE    ((byte)82),
   REQ_BROWSER_NEXTMESSAGEBLOCK   ((byte)83),
   RESP_BROWSER_NEXTMESSAGEBLOCK  ((byte)84),
   REQ_BROWSER_NEXTMESSAGE        ((byte)85),
   RESP_BROWSER_NEXTMESSAGE       ((byte)86),

   // Misc
   REQ_CLOSING                    ((byte)90),
   RESP_CLOSING                   ((byte)91),
   MSG_CLOSE                      ((byte)92);
   
   private final byte type;

   PacketType(byte type)
   {
      this.type = type;
   }

   public byte byteValue()
   {
      return type;
   }
}
