using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JBoss.JBM.Client.exception
{
    public class MessagingException : Exception
    {
        // Error codes -------------------------------------------------

        public const int INTERNAL_ERROR = 000;

        public const int UNSUPPORTED_PACKET = 001;

        public const int NOT_CONNECTED = 002;

        public const int CONNECTION_TIMEDOUT = 003;

        public const int INTERRUPTED = 004;


        public const int QUEUE_DOES_NOT_EXIST = 100;

        public const int QUEUE_EXISTS = 101;

        public const int OBJECT_CLOSED = 102;

        public const int INVALID_FILTER_EXPRESSION = 103;

        public const int ILLEGAL_STATE = 104;

        public const int SECURITY_EXCEPTION = 105;

        public const int ADDRESS_DOES_NOT_EXIST = 106;

        public const int ADDRESS_EXISTS = 107;

        public const int INCOMPATIBLE_CLIENT_SERVER_VERSIONS = 108;

        public const int SESSION_EXISTS = 109;

        private int code;

        public MessagingException()
        {
        }

        public MessagingException(int code)
        {
            this.code = code;
        }

        public MessagingException(int code, String msg)
            : base(msg)
        {
            this.code = code;
        }

        public MessagingException(int code, String msg, Exception cause)
            : base(msg, cause)
        {
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }

        public String toString()
        {
            return "MessagingException[errorCode=" + code + " message=" + this.Message + "]";
        }

    }
}
