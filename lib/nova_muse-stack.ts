import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { AttributeType, Table, BillingMode } from "aws-cdk-lib/aws-dynamodb";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "node:path";

export class NovaMuseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const table = new Table(this, "QuotesTable", {
      tableName: "NovaMuseQuotes",
      partitionKey: { name: "PK", type: AttributeType.STRING },
      sortKey: { name: "SK", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });
    table.addGlobalSecondaryIndex({
      indexName: "GSI1-Genre",
      partitionKey: { name: "GSI1PK", type: AttributeType.STRING },
      sortKey: { name: "GSI1SK", type: AttributeType.STRING },
    });
    table.addGlobalSecondaryIndex({
      indexName: "GSI2-Author",
      partitionKey: { name: "GSI2PK", type: AttributeType.STRING },
      sortKey: { name: "GSI2SK", type: AttributeType.STRING },
    });

    const quotesLambda = new lambda.Function(this, "QuotesLambda", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "quotes_handler.lambda_handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
      environment: {
        QUOTES_TABLE: table.tableName,
      },
    });

    const createQuotesLambda = new lambda.Function(this, "CreateQuotesLambda", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "createquotes_handler.lambda_handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
      environment: {
        QUOTES_TABLE: table.tableName,
      },
    });

    const api = new apigateway.RestApi(this, "NovaMuseApi", {
      restApiName: "NovaMuse Quotes Service",
      description: "Serves inspirational sci-fi and fantasy quotes",
      defaultCorsPreflightOptions: {
        allowOrigins: [
          "http://localhost:3000",
          "http://localhost:5173",
          "https://novamusequotes.c3devs.com",
        ],
        allowMethods: ["GET", "POST", "OPTIONS"],
        allowHeaders: ["Content-Type", "Authorization"],
      },
    });

    const quoteResource = api.root.addResource("quote");
    quoteResource.addMethod(
      "GET",
      new apigateway.LambdaIntegration(quotesLambda)
    );
    quoteResource.addMethod(
      "POST",
      new apigateway.LambdaIntegration(createQuotesLambda)
    );

    table.grantReadWriteData(createQuotesLambda);
    table.grantReadData(quotesLambda);
  }
}
