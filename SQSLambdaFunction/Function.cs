using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.SQSEvents;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace SQSLambdaFunction
{
    public class Function
    {
        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Function()
        {
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task Handler(SQSEvent evnt, ILambdaContext context)
        {
            var recordCount = evnt.Records.Count;
            var processId = Guid.NewGuid();
            foreach (var message in evnt.Records)
            {
                try
                {
                    context.Logger.LogLine($"#{recordCount--}.  Id {processId}");
                    await ProcessLambdaMessageAsync(message, context);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Find Me:{ex}");
                    throw ex;
                }
            }
            Console.WriteLine($"{evnt.Records.Count} message(s) completed");
        }


        public async Task ProcessLambdaMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            if (message.MessageAttributes != null)
                context.Logger.LogLine(string.Join(";",
                    message.MessageAttributes.Select(s => $"{s.Key}: {s.Value.StringValue}")));

            context.Logger.LogLine($"Processed message {message.Body}");


        }
    }
}
