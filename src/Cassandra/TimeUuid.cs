﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents a v1 uuid 
    /// </summary>
    public struct TimeUuid : IEquatable<TimeUuid>
    {
        private static readonly DateTimeOffset GregorianCalendarTime = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);
        //Reuse the random generator to avoid collisions
        private static readonly Random RandomGenerator = new Random();
        private static readonly object RandomLock = new object();

        private readonly Guid _value;

        private TimeUuid(Guid value)
        {
            _value = value;
        }

        private TimeUuid(byte[] nodeId, byte[] clockId, DateTimeOffset time)
        {
            if (nodeId == null || nodeId.Length != 6)
            {
                throw new ArgumentException("node id should contain 6 bytes");
            }
            if (clockId == null || clockId.Length != 2)
            {
                throw new ArgumentException("node id should contain 6 bytes");
            }
            var timeBytes = BitConverter.GetBytes((time - GregorianCalendarTime).Ticks);
            var buffer = new byte[16];
            //Positions 0-7 Timestamp
            Buffer.BlockCopy(timeBytes, 0, buffer, 0, 8);
            //Position 8-9 Clock
            Buffer.BlockCopy(clockId, 0, buffer, 8, 2);
            //Positions 10-15 Node
            Buffer.BlockCopy(nodeId, 0, buffer, 10, 6);

            //Version Byte: Time based
            //0001xxxx
            //turn off first 4 bits
            buffer[7] &= 0x0f; //00001111
            //turn on fifth bit
            buffer[7] |= 0x10; //00010000

            //Variant Byte: 1.0.x
            //10xxxxxx
            //turn off first 2 bits
            buffer[8] &= 0x3f; //00111111
            //turn on first bit
            buffer[8] |= 0x80; //10000000

            _value = new Guid(buffer);
        }

        /// <summary>
        /// Returns a value indicating whether this instance and a specified TimeUuid object represent the same value.
        /// </summary>
        public bool Equals(TimeUuid other)
        {
            return _value.Equals(other._value);
        }

        /// <summary>
        /// Returns a value indicating whether this instance and a specified TimeUuid object represent the same value.
        /// </summary>
        public override bool Equals(object obj)
        {
            var otherTimeUuid = obj as TimeUuid?;
            return otherTimeUuid != null && Equals(otherTimeUuid.Value);
        }

        /// <summary>
        /// Gets the DateTimeOffset representation of this instance
        /// </summary>
        public DateTimeOffset GetDate()
        {
            var bytes = _value.ToByteArray();
            //Remove version bit
            bytes[7] &= 0x0f; //00001111
            //Remove variant
            bytes[8] &= 0x3f; //00111111

            long timestamp = BitConverter.ToInt64(bytes, 0);
            long ticks = timestamp + GregorianCalendarTime.Ticks;

            return new DateTimeOffset(ticks, TimeSpan.Zero);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        /// <summary>
        /// Returns a 16-element byte array that contains the value of this instance.
        /// </summary>
        public byte[] ToByteArray()
        {
            return this._value.ToByteArray();
        }

        /// <summary>
        /// Gets the Guid representation of the Id
        /// </summary>
        public Guid ToGuid()
        {
            return _value;
        }

        /// <summary>
        /// Returns a string representation of the value of this instance in registry format.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return _value.ToString();
        }

        /// <summary>
        /// Returns a string representation
        /// </summary>
        public string ToString(string format, IFormatProvider provider)
        {
            return _value.ToString(format, provider);
        }

        /// <summary>
        /// Initializes a new instance of the TimeUuid structure, using a random node id and clock sequence and the current date time
        /// </summary>
        public static TimeUuid NewId()
        {
            return NewId(DateTimeOffset.Now);
        }

        /// <summary>
        /// Initializes a new instance of the TimeUuid structure, using a random node id and clock sequence
        /// </summary>
        public static TimeUuid NewId(DateTimeOffset date)
        {
            byte[] nodeId;
            byte[] clockId;
            lock (RandomLock)
            {
                //oh yeah, thread safety
                nodeId = new byte[6];
                clockId = new byte[2];
                RandomGenerator.NextBytes(nodeId);
                RandomGenerator.NextBytes(clockId);
            }
            return new TimeUuid(nodeId, clockId, date);
        }

        /// <summary>
        /// Initializes a new instance of the TimeUuid structure
        /// </summary>
        public static TimeUuid NewId(byte[] nodeId, byte[] clockId, DateTimeOffset date)
        {
            return new TimeUuid(nodeId, clockId, date);
        }

        /// <summary>
        /// From TimeUuid to Guid
        /// </summary>
        public static implicit operator Guid(TimeUuid value)
        {
            return value.ToGuid();
        }

        /// <summary>
        /// From Guid to TimeUuid
        /// </summary>
        public static implicit operator TimeUuid(Guid value)
        {
            return new TimeUuid(value);
        }

        public static bool operator ==(TimeUuid id1, TimeUuid id2)
        {
            return id1.ToGuid() == id2.ToGuid();
        }

        public static bool operator !=(TimeUuid id1, TimeUuid id2)
        {
            return id1.ToGuid() != id2.ToGuid();
        }
    }
}
