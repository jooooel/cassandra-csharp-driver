//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Responses
{
    internal class ResultResponse : Response
    {
        public enum ResultResponseKind
        {
            Void = 1,
            Rows = 2,
            SetKeyspace = 3,
            Prepared = 4,
            SchemaChange = 5
        };

        public const byte OpCode = 0x08;

        /// <summary>
        /// Cassandra result kind
        /// </summary>
        public ResultResponseKind Kind { get; private set; }

        /// <summary>
        /// Output of the result response based on the kind of result
        /// </summary>
        public IOutput Output { get; private set; }

        /// <summary>
        /// When the Output is ROWS, it returns the new_metadata_id.
        /// It returns null when new_metadata_id is not provided or the output is not ROWS.
        /// </summary>
        internal byte[] NewResultMetadataId => (Output as OutputRows)?.NewResultMetadataId;

        internal ResultResponse(Frame frame) : base(frame)
        {
            Kind = (ResultResponseKind) Reader.ReadInt32();
            switch (Kind)
            {
                case ResultResponseKind.Void:
                    Output = new OutputVoid(TraceId);
                    break;
                case ResultResponseKind.Rows:
                    Output = new OutputRows(Reader, TraceId);
                    break;
                case ResultResponseKind.SetKeyspace:
                    Output = new OutputSetKeyspace(Reader.ReadString());
                    break;
                case ResultResponseKind.Prepared:
                    Output = new OutputPrepared(frame.Header.Version, Reader);
                    break;
                case ResultResponseKind.SchemaChange:
                    Output = new OutputSchemaChange(Reader, TraceId);
                    break;
                default:
                    throw new DriverInternalError("Unknown ResultResponseKind Type");
            }
        }

        internal static ResultResponse Create(Frame frame)
        {
            return new ResultResponse(frame);
        }
    }
}
