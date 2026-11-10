/**
 * Feature: ElevenLabs Conversational AI Integration
 *
 * Demonstrates integrating ElevenLabs Conversational AI with Twilio Agent Connect.
 * Routes voice calls through TAC's VoiceChannel while leveraging ElevenLabs' agent
 * capabilities for natural conversation.
 *
 * Requires ElevenLabs API credentials in addition to the usual TAC env vars.
 */

import { config } from 'dotenv';
import {
  TAC,
  TACConfig,
  VoiceChannel,
  ConversationSession,
  ConversationId,
  ChannelType,
  ProfileId,
  TACServer,
  TACMemoryResponse,
  SMSChannel,
  ProfileResponse,
  ObservationInfo,
  SummaryInfo,
} from 'twilio-agent-connect';
import WebSocket from 'ws';

config({ path: '../.env' });

// ElevenLabs configuration
const ELEVENLABS_AGENT_ID = process.env.ELEVENLABS_AGENT_ID;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;

if (!ELEVENLABS_API_KEY) {
  console.error('Error: ELEVENLABS_API_KEY environment variable not set');
  process.exit(1);
}

if (!ELEVENLABS_AGENT_ID) {
  console.error('Error: ELEVENLABS_AGENT_ID environment variable not set');
  process.exit(1);
}

// Type definitions for ElevenLabs messages
interface SignedUrlResponse {
  signed_url: string;
}

interface ConversationInitMessage {
  type: 'conversation_initiation_client_data';
  conversation_config_override?: {
    agent?: {
      prompt?: {
        prompt?: string;
        llm?: string;
      };
      first_message?: string;
      language?: string;
    };
    tts?: {
      voice_id?: string;
    };
    conversation?: {
      text_only?: boolean;
    };
  };
  custom_llm_extra_body?: {
    temperature?: number;
    max_tokens?: number;
    [key: string]: any;
  };
  dynamic_variables?: {
    [key: string]: string | number | boolean;
  };
  user_id?: string;
  branch_id?: string;
  environment?: string;
}

interface UserMessage {
  type: 'user_message';
  text: string;
}

interface ElevenLabsMessage {
  type: string;
  agent_response_event?: {
    agent_response: string;
    event_id: number;
  };
}

// Disable SSL certificate validation for development (corporate proxy issue)
// WARNING: Do not use in production!
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

// Store WebSocket connections per conversation
const elevenLabsConnections = new Map<string, WebSocket>();

/**
 * Initialize ElevenLabs conversation and return WebSocket connection
 */
async function initializeElevenLabsConnection(
  conversationId: string,
  profile?: ProfileResponse,
  observations?: ObservationInfo[],
  summaries?: SummaryInfo[]
): Promise<WebSocket> {
  logger.info({ conversationId, profile }, 'Initializing ElevenLabs connection');

  // Get signed URL from ElevenLabs
  const url = `https://api.elevenlabs.io/v1/convai/conversation/get-signed-url?agent_id=${ELEVENLABS_AGENT_ID}`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'xi-api-key': ELEVENLABS_API_KEY!,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`ElevenLabs API returned ${response.status}: ${await response.text()}`);
  }

  const data = (await response.json()) as SignedUrlResponse;
  logger.info({ conversationId }, 'Got signed URL from ElevenLabs');

  // Connect to ElevenLabs WebSocket
  const ws = new WebSocket(data.signed_url);

  return new Promise((resolve, reject) => {
    ws.on('open', () => {
      logger.info(
        {
          conversationId,
          hasProfile: !!profile,
          observationCount: observations?.length ?? 0,
          summaryCount: summaries?.length ?? 0,
        },
        'Connected to ElevenLabs agent'
      );

      // Send conversation config override for text-only mode
      const initMessage: ConversationInitMessage = {
        type: 'conversation_initiation_client_data',
        conversation_config_override: {
          conversation: {
            text_only: true,
          },
        },
        dynamic_variables: {
          profile: profile ? JSON.stringify(profile) : '',
          observations: observations ? JSON.stringify(observations) : '',
          summaries: summaries ? JSON.stringify(summaries) : '',
        },
      };

      ws.send(JSON.stringify(initMessage));
      elevenLabsConnections.set(conversationId, ws);
      resolve(ws);
    });

    ws.on('error', error => {
      logger.error({ err: error, conversationId }, 'ElevenLabs WebSocket error');
      reject(error);
    });

    ws.on('close', () => {
      logger.info({ conversationId }, 'ElevenLabs connection closed');
      elevenLabsConnections.delete(conversationId);
    });
  });
}

/**
 * Send message to ElevenLabs and wait for response
 */
async function sendToElevenLabs(
  conversationId: string,
  message: string,
  profile: ProfileResponse | undefined,
  observations: ObservationInfo[] | undefined,
  summaries: SummaryInfo[] | undefined
): Promise<string> {
  let ws = elevenLabsConnections.get(conversationId);

  // Initialize connection if it doesn't exist
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    ws = await initializeElevenLabsConnection(conversationId, profile, observations, summaries);
  }

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('ElevenLabs response timeout'));
    }, 30000); // 30 second timeout

    // Set up response handler
    const messageHandler = (data: WebSocket.RawData) => {
      try {
        const response = JSON.parse(data.toString()) as ElevenLabsMessage;
        logger.debug({ conversationId, messageType: response.type }, 'Received ElevenLabs message');

        if (response.type === 'agent_response' && response.agent_response_event) {
          clearTimeout(timeout);
          ws!.off('message', messageHandler);
          logger.info(
            { conversationId, response: response.agent_response_event.agent_response },
            'Received agent response'
          );
          resolve(response.agent_response_event.agent_response);
        }
      } catch (err) {
        logger.error({ err, conversationId }, 'Error parsing ElevenLabs message');
      }
    };

    ws.on('message', messageHandler);

    // Send user message
    const userMessage: UserMessage = {
      type: 'user_message',
      text: message,
    };

    logger.info({ conversationId, message }, 'Sending message to ElevenLabs agent');
    ws.send(JSON.stringify(userMessage));
  });
}

const tac = await TAC.create({ config: TACConfig.fromEnv() });
const logger = tac.logger.child({ component: 'elevenlabs' });

const voiceChannel = new VoiceChannel(tac, { memoryMode: 'once' });
const smsChannel = new SMSChannel(tac, { memoryMode: 'always' });

tac.registerChannel(smsChannel);
tac.registerChannel(voiceChannel);

async function handleMessageReady(params: {
  conversationId: ConversationId;
  profileId: ProfileId | undefined;
  message: string;
  author: string;
  memory: TACMemoryResponse | undefined;
  session: ConversationSession;
  channel: ChannelType;
}): Promise<string | undefined> {
  const { conversationId, message, author, memory, channel, profileId } = params;

  logger.info({ conversationId, message, author, channel }, 'Received message');

  const observations = memory?.observations;
  const summaries = memory?.summaries;
  const profile = profileId ? await tac.fetchProfile(profileId) : undefined;

  try {
    // Send to ElevenLabs and get response
    const response = await sendToElevenLabs(
      conversationId,
      message,
      profile,
      observations,
      summaries
    );
    return response;
  } catch (error) {
    logger.error({ err: error, conversationId }, 'Error getting ElevenLabs response');
    return 'I apologize, but I encountered an error processing your request.';
  }
}

tac.onMessageReady(handleMessageReady);

// Clean up connections on conversation end
tac.onConversationEnded(({ session }) => {
  const { conversationId } = session;
  console.log(`Conversation ended: ${conversationId}`);
  const ws = elevenLabsConnections.get(conversationId);
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.close();
  }
  elevenLabsConnections.delete(conversationId);
});

const server = new TACServer(tac);

server
  .start()
  .then(() => {
    console.log('ElevenLabs Agent Connect server started successfully');
  })
  .catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
