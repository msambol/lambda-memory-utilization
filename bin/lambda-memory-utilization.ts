#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { LambdaMemoryUtilizationStack } from '../lib/lambda-memory-utilization-stack';

const app = new cdk.App();
new LambdaMemoryUtilizationStack(app, 'LambdaMemoryUtilizationStack', {});
