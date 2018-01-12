using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;


using Amazon.Rekognition;
using Amazon.Rekognition.Model;

using Amazon.S3;
using Amazon.S3.Model;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Rekognize
{
    public class Function
    {
        /// <summary>
        /// The default minimum confidence used for detecting labels.
        /// </summary>
        public const float DEFAULT_MIN_CONFIDENCE = 70f;

        /// <summary>
        /// The name of the environment variable to set which will override the default minimum confidence level.
        /// </summary>
        public const string MIN_CONFIDENCE_ENVIRONMENT_VARIABLE_NAME = "MinConfidence";

        public const string USER_PHOTOS_TABLE_NAME = "Photos";

        public const string FACES_TABLE_NAME = "Faces";

        public const string DUMP_COLLECTION_NAME = "Dump";

        IAmazonS3 S3Client { get; }

        IAmazonRekognition RekognitionClient { get; }

        IAmazonDynamoDB DynamoDBClient { get; }

        float MinConfidence { get; set; } = DEFAULT_MIN_CONFIDENCE;

        HashSet<string> SupportedImageTypes { get; } = new HashSet<string> { ".png", ".jpg", ".jpeg", ".PNG", ".JPG", ".JPEG" };

        /// <summary>
        /// Default constructor used by AWS Lambda to construct the function. Credentials and Region information will
        /// be set by the running Lambda environment.
        /// 
        /// This constuctor will also search for the environment variable overriding the default minimum confidence level
        /// for label detection.
        /// </summary>
        public Function()
        {
            this.S3Client = new AmazonS3Client();
            this.RekognitionClient = new AmazonRekognitionClient();
            this.DynamoDBClient = new AmazonDynamoDBClient();

            var environmentMinConfidence = System.Environment.GetEnvironmentVariable(MIN_CONFIDENCE_ENVIRONMENT_VARIABLE_NAME);
            if (!string.IsNullOrWhiteSpace(environmentMinConfidence))
            {
                float value;
                if (float.TryParse(environmentMinConfidence, out value))
                {
                    this.MinConfidence = value;
                    Console.WriteLine($"Setting minimum confidence to {this.MinConfidence}");
                }
                else
                {
                    Console.WriteLine($"Failed to parse value {environmentMinConfidence} for minimum confidence. Reverting back to default of {this.MinConfidence}");
                }
            }
            else
            {
                Console.WriteLine($"Using default minimum confidence of {this.MinConfidence}");
            }
        }

        /// <summary>
        /// Constructor used for testing which will pass in the already configured service clients.
        /// </summary>
        /// <param name="s3Client"></param>
        /// <param name="rekognitionClient"></param>
        /// <param name="minConfidence"></param>
        public Function(IAmazonS3 s3Client, IAmazonRekognition rekognitionClient, float minConfidence)
        {
            this.S3Client = s3Client;
            this.RekognitionClient = rekognitionClient;
            this.MinConfidence = minConfidence;
        }

        /// <summary>
        /// A function for responding to S3 create events. It will determine if the object is an image and use Amazon Rekognition
        /// to detect labels and add the labels as tags on the S3 object.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event input, ILambdaContext context)
        {
            foreach (var record in input.Records)
            {
                int foundCount = 0;
                if (!SupportedImageTypes.Contains(Path.GetExtension(record.S3.Object.Key)))
                {
                    Console.WriteLine($"Object {record.S3.Bucket.Name}:{record.S3.Object.Key} is not a supported image type");
                    continue;
                }

                var bucket = record.S3.Bucket.Name;
                var key = record.S3.Object.Key;

                Console.WriteLine($"Processing image {key} from bucket {bucket}");

                var metadataResponse = await this.S3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = bucket,
                    Key = key
                });

                var user = metadataResponse.Metadata["x-amz-meta-user"];

                if (user != null)
                {
                    var listFacesRes = await this.RekognitionClient.ListFacesAsync(new ListFacesRequest
                    {
                        CollectionId = user
                    });
                    foreach (var face in listFacesRes.Faces)
                    {
                        Console.WriteLine(face.FaceId);
                    }

                    var detectFacesResponses = await this.RekognitionClient.IndexFacesAsync(new IndexFacesRequest
                    {
                        CollectionId = user,
                        Image = new Image
                        {
                            S3Object = new Amazon.Rekognition.Model.S3Object
                            {
                                Bucket = bucket,
                                Name = key
                            }
                        }
                    });

                    Console.WriteLine($"Detected {detectFacesResponses.FaceRecords.Count} faces");
                    listFacesRes = await this.RekognitionClient.ListFacesAsync(new ListFacesRequest
                    {
                        CollectionId = user
                    });
                    foreach (var face in listFacesRes.Faces)
                    {
                        Console.WriteLine(face.FaceId);
                    }
                    if (detectFacesResponses.FaceRecords.Count > 0)
                    {
                        foreach (var detectedFace in detectFacesResponses.FaceRecords)
                        {
                            SearchFacesResponse searchFacesReponse = null;
                            try
                            {
                                Console.WriteLine($"Col {user} - Faceid {detectedFace.Face.FaceId} ");
                                searchFacesReponse = await this.RekognitionClient.SearchFacesAsync(new SearchFacesRequest
                                {
                                    CollectionId = user,
                                    FaceId = detectedFace.Face.FaceId,
                                    FaceMatchThreshold = 80f,
                                    MaxFaces = 1
                                });
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message + "\n" + e.GetType().ToString());
                            }
                            if (searchFacesReponse != null)
                            {
                                Console.WriteLine($"Matched {searchFacesReponse.FaceMatches.Count} faces");
                                var match = searchFacesReponse.FaceMatches.FirstOrDefault();
                                if (match != null)
                                {
                                    try
                                    {
                                        Table facesTable = Table.LoadTable(this.DynamoDBClient, FACES_TABLE_NAME);
                                        var detectedUser = await facesTable.GetItemAsync(match.Face.FaceId);
                                        Console.WriteLine($"Matched faces {detectedFace.Face.FaceId} with {match.Face.FaceId} confidence {match.Similarity}");
                                        foundCount++;
                                        var updateRequest = new UpdateItemRequest
                                        {
                                            TableName = USER_PHOTOS_TABLE_NAME,
                                            Key = new Dictionary<string, AttributeValue>() { { "Face", new AttributeValue { S = detectedUser["Email"] } } },
                                            ExpressionAttributeNames = new Dictionary<string, string>()
                                    {
                                        {"#P", "Photo" }
                                    },
                                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                                    {
                                        {":photokey", new AttributeValue { SS = {key} }}
                                    },
                                            UpdateExpression = "ADD #P :photokey"

                                        };
                                        await this.DynamoDBClient.UpdateItemAsync(updateRequest);
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine(e.Message + "\n" + e.GetType().ToString());
                                    }
                                }
                            }
                        }
                    }
                    var cleanup = await this.RekognitionClient.DeleteFacesAsync(new DeleteFacesRequest
                    {
                        CollectionId = user,
                        FaceIds = detectFacesResponses.FaceRecords.Select(x => x.Face.FaceId).ToList()
                    });
                    Console.WriteLine($"Found a match for {foundCount} out of {detectFacesResponses.FaceRecords.Count} detected faces");
                }
                else
                {
                    Console.WriteLine("There was no username metadata");
                }
                return;
            }
        }
    }
}
