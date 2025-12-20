import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import * as NovaMuse from '../lib/nova_muse-stack.ts';
import { test } from '@jest/globals';

test('DynamoDB Table Created', () => {
  const app = new cdk.App();
  const stack = new NovaMuse.NovaMuseStack(app, 'MyTestStack');
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::DynamoDB::Table', {
    TableName: 'NovaMuseQuotes'
  });
});
