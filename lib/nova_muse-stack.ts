import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { AttributeType, Table, BillingMode } from "aws-cdk-lib/aws-dynamodb";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "node:path";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as targets from "aws-cdk-lib/aws-route53-targets";

export class NovaMuseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const certArn = process.env.ACM_ARN;
    if (!certArn) {
      throw new Error("ACM_ARN environment variable must be set");
    }

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

    const browseQuotesLambda = new lambda.Function(this, "BrowseQuotesLambda", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "browsequotes_handler.lambda_handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
      environment: {
        QUOTES_TABLE: table.tableName,
      },
    });

    const userPool = new cognito.UserPool(this, "NovaMuseUserPool", {
      userPoolName: "NovaMuseUsers",
      signInAliases: {
        email: true,
      },
      selfSignUpEnabled: false, // YOU control who joins
      passwordPolicy: {
        minLength: 12,
        requireLowercase: true,
        requireUppercase: true,
        requireDigits: true,
        requireSymbols: true,
      },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
    });

    new cognito.CfnUserPoolGroup(this, "AdminsGroup", {
      userPoolId: userPool.userPoolId,
      groupName: "admins",
      description: "Admin users who can create/edit quotes",
    });

    const userPoolClient = userPool.addClient("NovaMuseAppClient", {
      authFlows: {
        userPassword: true,
        userSrp: true,
      },
      generateSecret: false,
      oAuth: {
        flows: {
          authorizationCodeGrant: true,
        },
        scopes: [
          cognito.OAuthScope.EMAIL,
          cognito.OAuthScope.OPENID,
          cognito.OAuthScope.PROFILE,
        ],
        callbackUrls: [
          "http://localhost:5173", // dev
          "https://novamusequotes.c3devs.com", // prod
        ],
        logoutUrls: [
          "http://localhost:5173",
          "https://novamusequotes.c3devs.com",
        ],
      },
    });

    userPool.addDomain("NovaMuseCognitoDomain", {
      cognitoDomain: {
        domainPrefix: "novamuse",
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

    const authorizer = new apigateway.CognitoUserPoolsAuthorizer(
      this,
      "NovaMuseAuthorizer",
      {
        cognitoUserPools: [userPool],
      }
    );

    authorizer._attachToApi(api);

    const quoteResource = api.root.addResource("quote");
    quoteResource.addMethod(
      "GET",
      new apigateway.LambdaIntegration(quotesLambda)
    );
    quoteResource.addMethod(
      "POST",
      new apigateway.LambdaIntegration(createQuotesLambda),
      {
        authorizer,
        authorizationType: apigateway.AuthorizationType.COGNITO,
      }
    );
    quoteResource
      .addResource("browse")
      .addMethod("GET", new apigateway.LambdaIntegration(browseQuotesLambda));

    table.grantReadWriteData(createQuotesLambda);
    table.grantReadData(quotesLambda);
    table.grantReadData(browseQuotesLambda);

    new cdk.CfnOutput(this, "CognitoLoginUrl", {
      value: `https://novamuse.auth.${this.region}.amazoncognito.com/login?client_id=${userPoolClient.userPoolClientId}&response_type=code&scope=email+openid+profile&redirect_uri=https://novamusequotes.c3devs.com`,
    });

    const hostedZone = route53.HostedZone.fromLookup(this, "HostedZone", {
      domainName: "c3devs.com",
    });

    const wildcardCert = acm.Certificate.fromCertificateArn(
      this,
      "WildcardCert",
      certArn
    );

    const apiDomain = new apigateway.DomainName(this, "NovaMuseApiDomain", {
      domainName: "novamuseapi.c3devs.com",
      certificate: wildcardCert,
      endpointType: apigateway.EndpointType.REGIONAL,
      securityPolicy: apigateway.SecurityPolicy.TLS_1_2,
    });

    new apigateway.BasePathMapping(this, "NovaMuseApiMapping", {
      domainName: apiDomain,
      restApi: api,
      stage: api.deploymentStage,
    });

    new route53.ARecord(this, "NovaMuseApiAliasRecord", {
      zone: hostedZone,
      recordName: "novamuseapi",
      target: route53.RecordTarget.fromAlias(
        new targets.ApiGatewayDomain(apiDomain)
      ),
    });
  }
}
