import { Inject } from '@nestjs/common';
import {
  getConnectionToken,
  getJetStreamClientToken,
  getNatsClientToken,
  getStreamManagerToken,
} from './tokens.js';

export const InjectNatsClient = (name?: string): ParameterDecorator & PropertyDecorator =>
  Inject(getNatsClientToken(name));

export const InjectNatsConnection = (name?: string): ParameterDecorator & PropertyDecorator =>
  Inject(getConnectionToken(name));

export const InjectJetStream = (name?: string): ParameterDecorator & PropertyDecorator =>
  Inject(getJetStreamClientToken(name));

export const InjectStreamManager = (name?: string): ParameterDecorator & PropertyDecorator =>
  Inject(getStreamManagerToken(name));
