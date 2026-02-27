'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import DailyIframe, { DailyCall } from '@daily-co/daily-js';
import { ConnectionStatus } from '@/components/ConnectionStatus';
import { VideoConsultation } from '@/components/VideoConsultation';
import { TranscriptPanel, TranscriptMessage } from '@/components/TranscriptPanel';

type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error';

type DailyParticipant = {
  local?: boolean;
  tracks?: {
    audio?: { state?: string; persistentTrack?: MediaStreamTrack | null };
    customAudio?: { state?: string; persistentTrack?: MediaStreamTrack | null };
    video?: { state?: string; persistentTrack?: MediaStreamTrack | null };
  };
};

export default function Home() {
  const [connectionStatus, setConnectionStatus] = useState<ConnectionState>('disconnected');
  const [micEnabled, setMicEnabled] = useState(true);
  const [cameraEnabled, setCameraEnabled] = useState(true);
  const [screenSharing, setScreenSharing] = useState(false);
  const [messages, setMessages] = useState<TranscriptMessage[]>([]);
  const [localStream, setLocalStream] = useState<MediaStream | null>(null);
  const [remoteStream, setRemoteStream] = useState<MediaStream | null>(null);
  const [needsAudioGesture, setNeedsAudioGesture] = useState(false);
  const [xrayFile, setXrayFile] = useState<File | null>(null);
  const [xrayUploading, setXrayUploading] = useState(false);
  const [xrayUploadStatus, setXrayUploadStatus] = useState<string | null>(null);
  const [xrayUploadError, setXrayUploadError] = useState<string | null>(null);
  const [xrayPreviewUrl, setXrayPreviewUrl] = useState<string | null>(null);
  const [xrayStatusIndex, setXrayStatusIndex] = useState(0);

  const callObjectRef = useRef<DailyCall | null>(null);
  const botAudioRef = useRef<HTMLAudioElement | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const currentRoomUrlRef = useRef<string | null>(null);

  const xrayUploadEndpoint =
    process.env.NEXT_PUBLIC_XRAY_UPLOAD_ENDPOINT ?? 'http://localhost:8000/upload_xray';
  const apiBase =
    process.env.NEXT_PUBLIC_API_BASE ??
    (xrayUploadEndpoint.endsWith('/upload_xray')
      ? xrayUploadEndpoint.replace(/\/upload_xray$/, '')
      : 'http://localhost:8000');

  const xrayStatusMessages = [
    'Scanning image parameters...',
    'Analyzing hemoglobin indicators...',
    'Validating imaging quality...',
    'Finalizing triage score...',
  ];

  const addMessage = useCallback((speaker: 'user' | 'bot', text: string) => {
    const trimmed = text.trim();
    if (!trimmed) return;
    const newMessage: TranscriptMessage = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      speaker,
      text: trimmed,
      timestamp: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, newMessage]);
  }, []);

  const updateMediaStreams = useCallback(() => {
    const callObject = callObjectRef.current;
    if (!callObject) return;

    const participants = callObject.participants() as Record<string, DailyParticipant>;
    const localParticipant = participants.local;
    const localVideo = localParticipant?.tracks?.video;

    if (localVideo?.state === 'playable' && localVideo.persistentTrack) {
      setLocalStream(new MediaStream([localVideo.persistentTrack]));
    } else {
      setLocalStream(null);
    }

    const remoteEntry = Object.entries(participants).find(([, p]) => !p.local);
    const remoteParticipant = remoteEntry?.[1];
    const remoteParticipantId = remoteEntry?.[0];
    const remoteAudio = remoteParticipant?.tracks?.audio;
    const remoteCustomAudio = remoteParticipant?.tracks?.customAudio;
    console.log('Daily remote track states', {
      remoteAudio: remoteAudio?.state,
      remoteCustomAudio: remoteCustomAudio?.state,
    });
    void remoteParticipantId;
    const chosenTrack =
      remoteAudio?.state === 'playable'
        ? remoteAudio?.persistentTrack
        : remoteCustomAudio?.state === 'playable'
          ? remoteCustomAudio?.persistentTrack
          : null;

    if (chosenTrack) {
      const stream = new MediaStream([chosenTrack]);
      setRemoteStream(stream);
      if (botAudioRef.current) {
        botAudioRef.current.srcObject = stream;
        botAudioRef.current.muted = false;
        botAudioRef.current.volume = 1;
        setNeedsAudioGesture(true);
        botAudioRef.current
          .play()
          .then(() => setNeedsAudioGesture(false))
          .catch(() => setNeedsAudioGesture(true));
      }
    } else {
      setRemoteStream(null);
    }
  }, []);

  const attachRemoteAudioTrack = useCallback((track: MediaStreamTrack) => {
    const stream = new MediaStream([track]);
    setRemoteStream(stream);
    if (botAudioRef.current) {
      botAudioRef.current.srcObject = stream;
      botAudioRef.current.muted = false;
      botAudioRef.current.volume = 1;
      setNeedsAudioGesture(true);
      botAudioRef.current
        .play()
        .then(() => setNeedsAudioGesture(false))
        .catch(() => setNeedsAudioGesture(true));
    }
  }, []);

  const startSession = async () => {
    if (callObjectRef.current) return;
    if (connectionStatus === 'connected' || connectionStatus === 'connecting') return;

    setConnectionStatus('connecting');

    try {
      const roomResponse = await fetch(`${apiBase}/create-room`, {
        method: 'POST',
      });
      if (!roomResponse.ok) {
        throw new Error('Failed to create Daily room');
      }

      const roomData = await roomResponse.json();
      const roomUrl = roomData?.url as string | undefined;
      const token = roomData?.token as string | undefined;

      if (!roomUrl || !token) {
        throw new Error('Missing room URL or token');
      }
      currentRoomUrlRef.current = roomUrl;

      const startResponse = await fetch(`${apiBase}/start-bot`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ room_url: roomUrl, token }),
      });

      if (!startResponse.ok) {
        throw new Error('Failed to start bot');
      }

      const callObject = DailyIframe.createCallObject({
        subscribeToTracksAutomatically: false,
      });
      callObjectRef.current = callObject;

      callObject.on('joining-meeting', (event) => {
        console.log('Daily joining-meeting', event);
      });
      callObject.on('joined-meeting', (event) => {
        console.log('Daily joined-meeting', event);
        setConnectionStatus('connected');
      });
      callObject.on('left-meeting', (event) => {
        console.log('Daily left-meeting', event);
        setConnectionStatus('disconnected');
      });
      callObject.on('error', (event) => {
        console.error('Daily error', event);
        setConnectionStatus('error');
      });

      callObject.on('transcription-started', (event) => {
        console.log('Daily transcription-started', event);
      });
      callObject.on('transcription-message', (event: any) => {
        console.log('Daily transcription-message', event);
        const text =
          event?.text ??
          event?.transcript ??
          event?.payload?.text ??
          event?.payload?.transcript ??
          '';
        if (!text) return;

        const participantId =
          event?.participantId ??
          event?.participant_id ??
          event?.payload?.participantId ??
          event?.payload?.participant_id ??
          null;
        let speaker: 'user' | 'bot' = 'bot';
        if (participantId) {
          const participants = callObject.participants() as Record<string, DailyParticipant & { local?: boolean }>;
          if (participants?.[participantId]?.local) {
            speaker = 'user';
          }
        }
        addMessage(speaker, text);
      });

      if (!eventSourceRef.current) {
        const eventsUrl = `${apiBase}/events`;
        const eventSource = new EventSource(eventsUrl);
        eventSourceRef.current = eventSource;
        eventSource.onmessage = (evt) => {
          try {
            const payload = JSON.parse(evt.data);
            const roomUrlFromEvent = payload?.room_url ?? null;
            if (
              roomUrlFromEvent &&
              currentRoomUrlRef.current &&
              roomUrlFromEvent !== currentRoomUrlRef.current
            ) {
              return;
            }
            const text = payload?.text ?? '';
            if (text) addMessage('bot', text);
          } catch (error) {
            console.error('Failed to parse bot transcript event', error);
          }
        };
        eventSource.onerror = (error) => {
          console.error('Bot transcript event stream error', error);
        };
      }

      callObject.on('participant-joined', (event: any) => {
        console.log('Daily participant-joined', event);
        const participantId =
          event?.participant?.participantId ?? event?.participant?.id ?? event?.participant?.session_id;
        if (participantId && event?.participant?.local !== true) {
          try {
            callObject.updateParticipant(participantId, {
              setSubscribedTracks: true,
            });
          } catch (error) {
            console.error('Failed to subscribe to participant tracks', error);
          }
        }
        updateMediaStreams();
      });
      callObject.on('participant-updated', updateMediaStreams);
      callObject.on('participant-left', updateMediaStreams);
      callObject.on('track-started', (event: any) => {
        console.log('Daily track-started', {
          type: event?.type,
          trackKind: event?.track?.kind,
          trackLabel: event?.track?.label,
          participantLocal: event?.participant?.local,
          participantId: event?.participant?.participantId ?? event?.participant?.id,
        });
        const track = event?.track;
        const participant = event?.participant;
        if (!track || track.kind !== 'audio' || participant?.local === true) return;
        console.log('Daily remote audio track', {
          id: track.id,
          label: track.label,
          muted: track.muted,
          readyState: track.readyState,
          participant,
        });
        attachRemoteAudioTrack(track);
      });
      callObject.on('track-stopped', updateMediaStreams);

      try {
        callObject.setSubscribeToTracksAutomatically(false);
      } catch (error) {
        console.error('Failed to disable auto track subscriptions', error);
      }
      await callObject.join({ url: roomUrl, token });
      await callObject.setLocalAudio(micEnabled);
      await callObject.setLocalVideo(cameraEnabled);
      try {
        await callObject.startTranscription();
      } catch (error) {
        console.error('Failed to start transcription', error);
      }
    } catch (error) {
      console.error('Failed to start session:', error);
      setConnectionStatus('error');
    }
  };

  const stopSession = useCallback(async () => {
    const callObject = callObjectRef.current;
    if (callObject) {
      try {
        await callObject.leave();
      } catch (error) {
        console.error('Error leaving call:', error);
      }
      callObject.destroy();
      callObjectRef.current = null;
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    currentRoomUrlRef.current = null;

    if (localStream) {
      localStream.getTracks().forEach((track) => track.stop());
      setLocalStream(null);
    }

    if (remoteStream) {
      remoteStream.getTracks().forEach((track) => track.stop());
      setRemoteStream(null);
    }

    setConnectionStatus('disconnected');
    setScreenSharing(false);
    setMessages([]);
  }, [localStream, remoteStream]);


  const toggleMic = async () => {
    const callObject = callObjectRef.current;
    if (!callObject) return;

    try {
      await callObject.setLocalAudio(!micEnabled);
      setMicEnabled(!micEnabled);
    } catch (error) {
      console.error('Error toggling mic:', error);
    }
  };

  const toggleCamera = async () => {
    const callObject = callObjectRef.current;
    if (!callObject) return;

    try {
      await callObject.setLocalVideo(!cameraEnabled);
      setCameraEnabled(!cameraEnabled);
    } catch (error) {
      console.error('Error toggling camera:', error);
    }
  };

  const toggleScreenShare = async () => {
    const callObject = callObjectRef.current;
    if (!callObject) return;

    try {
      if (screenSharing) {
        await callObject.stopScreenShare();
      } else {
        await callObject.startScreenShare();
      }
      setScreenSharing(!screenSharing);
    } catch (error) {
      console.error('Error toggling screen share:', error);
    }
  };

  const uploadXray = async () => {
    if (!xrayFile) return;
    setXrayUploading(true);
    setXrayUploadStatus(null);
    setXrayUploadError(null);

    try {
      const formData = new FormData();
      formData.append('file', xrayFile);

      const response = await fetch(xrayUploadEndpoint, {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || 'Upload failed');
      }

      const data = await response.json();
      setXrayUploadStatus(data?.path ? `Uploaded: ${data.path}` : 'Upload complete');
      setXrayFile(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Upload failed';
      setXrayUploadError(message);
    } finally {
      setXrayUploading(false);
    }
  };

  useEffect(() => {
    if (!xrayFile) {
      setXrayPreviewUrl(null);
      return;
    }
    const url = URL.createObjectURL(xrayFile);
    setXrayPreviewUrl(url);
    return () => URL.revokeObjectURL(url);
  }, [xrayFile]);

  useEffect(() => {
    if (!xrayUploading) {
      setXrayStatusIndex(0);
      return;
    }
    const interval = setInterval(() => {
      setXrayStatusIndex((prev) => (prev + 1) % xrayStatusMessages.length);
    }, 2000);
    return () => clearInterval(interval);
  }, [xrayUploading, xrayStatusMessages.length]);

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Bot audio output */}
      <audio ref={botAudioRef} autoPlay playsInline className="hidden" />

      {/* Header */}
      <header className="fixed top-0 left-0 right-0 z-50 bg-white border-b border-slate-200">
        {/* BigTB6 logo - top left corner, outside centered container */}
        <div className="absolute left-20 top-0 h-16 flex items-center">
          <span className="text-xl font-medium text-slate-900">BigTB6</span>
        </div>

        {/* Right side - Connect button, status, and control buttons */}
        <div className="absolute right-0 top-0 h-16 px-4 sm:px-6 lg:px-8 flex items-center gap-3">
          <button
            onClick={connectionStatus === 'connected' ? stopSession : startSession}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors duration-150 ${
              connectionStatus === 'connected'
                ? 'bg-slate-100 text-slate-700 hover:bg-slate-200'
                : 'bg-[#099c8f] text-white hover:bg-[#07897d] active:bg-[#067a70]'
            }`}
          >
            {connectionStatus === 'connected' ? 'Disconnect' : 'Connect'}
          </button>
          <ConnectionStatus status={connectionStatus} />

          {/* Control buttons */}
          {/* Microphone Toggle */}
          <button
            onClick={toggleMic}
            disabled={connectionStatus !== 'connected'}
            className={`p-2 rounded-lg border border-slate-200 transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed ${
              micEnabled
                ? 'bg-white text-slate-700 hover:bg-slate-50'
                : 'bg-red-50 text-red-700 hover:bg-red-100'
            }`}
            aria-label={micEnabled ? 'Mute microphone' : 'Unmute microphone'}
            title={micEnabled ? 'Mute' : 'Unmute'}
          >
            {micEnabled ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7 7v4m0 0H8m4 0h4m-4-8a3 3 0 01-3-3V5a3 3 0 116 0v6a3 3 0 01-3 3z" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5.586 15H4a1 1 0 01-1-1v-4a1 1 0 011-1h1.586l4.707-4.707C10.923 3.663 12 4.109 12 5v14c0 .891-1.077 1.337-1.707.707L5.586 15z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2" />
              </svg>
            )}
          </button>

          {/* Camera Toggle */}
          <button
            onClick={toggleCamera}
            disabled={connectionStatus !== 'connected'}
            className={`p-2 rounded-lg border border-slate-200 transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed ${
              cameraEnabled
                ? 'bg-white text-slate-700 hover:bg-slate-50'
                : 'bg-red-50 text-red-700 hover:bg-red-100'
            }`}
            aria-label={cameraEnabled ? 'Turn off camera' : 'Turn on camera'}
            title={cameraEnabled ? 'Turn off camera' : 'Turn on camera'}
          >
            {cameraEnabled ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
              </svg>
            )}
          </button>

          {/* Screen Share Toggle */}
          <button
            onClick={toggleScreenShare}
            disabled={connectionStatus !== 'connected'}
            className={`p-2 rounded-lg border border-slate-200 transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed ${
              screenSharing
                ? 'bg-slate-100 text-slate-700 hover:bg-slate-200'
                : 'bg-white text-slate-700 hover:bg-slate-50'
            }`}
            aria-label={screenSharing ? 'Stop sharing screen' : 'Share screen'}
            title={screenSharing ? 'Stop sharing' : 'Share screen'}
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
            </svg>
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="pt-20 pb-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          {/* Video Consultation Area */}
          <VideoConsultation
            localStream={localStream}
            remoteStream={remoteStream}
            isConnected={connectionStatus === 'connected'}
            sidePanel={(
              <div className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">
                <div className="text-sm font-medium text-slate-900">Chest X-ray Upload</div>
                <p className="mt-1 text-xs text-slate-600">Upload an X-ray image for analysis.</p>

                <div className="mt-3 relative">
                  <div
                    className={`transition-opacity duration-300 ease-in-out ${
                      xrayUploading ? 'opacity-0 pointer-events-none' : 'opacity-100'
                    }`}
                  >
                    <input
                      type="file"
                      accept="image/*"
                      onChange={(e) => setXrayFile(e.target.files?.[0] ?? null)}
                      className="block w-full text-xs text-slate-600 file:mr-3 file:rounded-lg file:border-0 file:bg-slate-100 file:px-3 file:py-2 file:text-xs file:font-medium file:text-slate-700 hover:file:bg-slate-200"
                    />
                    <button
                      onClick={uploadXray}
                      disabled={!xrayFile || xrayUploading}
                      className="mt-3 w-full px-3 py-2 rounded-lg bg-[#099c8f] text-white text-xs font-medium disabled:opacity-60 disabled:cursor-not-allowed hover:bg-[#07897d] transition-colors duration-150"
                    >
                      Upload
                    </button>
                  </div>

                  <div
                    className={`absolute inset-0 transition-opacity duration-300 ease-in-out ${
                      xrayUploading ? 'opacity-100' : 'opacity-0 pointer-events-none'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      {xrayPreviewUrl ? (
                        <div className="w-12 h-12 rounded-lg border border-slate-200 overflow-hidden bg-slate-50 animate-breathe">
                          <img
                            src={xrayPreviewUrl}
                            alt="Uploaded chest x-ray"
                            className="w-full h-full object-cover"
                          />
                        </div>
                      ) : (
                        <div className="w-12 h-12 rounded-lg border border-slate-200 bg-slate-100 animate-breathe" />
                      )}
                      <div>
                        <p className="text-xs font-medium text-slate-800">Processing image</p>
                        <p className="text-xs text-slate-500">{xrayStatusMessages[xrayStatusIndex]}</p>
                      </div>
                    </div>
                    <div className="mt-3 h-1 w-full bg-slate-100 rounded-full overflow-hidden">
                      <div className="h-full w-1/3 bg-[#099c8f] animate-breathe" />
                    </div>
                  </div>
                </div>

                {xrayUploadStatus && (
                  <p className="mt-3 text-xs text-emerald-700 transition-opacity duration-300 ease-in-out opacity-100">
                    {xrayUploadStatus}
                  </p>
                )}
                {xrayUploadError && (
                  <p className="mt-3 text-xs text-red-600">{xrayUploadError}</p>
                )}
              </div>
            )}
          />

          {/* Transcript Panel */}
          <TranscriptPanel messages={messages} />

          {needsAudioGesture && (
            <div className="mt-4 text-center">
              <button
                onClick={() => {
                  botAudioRef.current?.play().then(() => setNeedsAudioGesture(false));
                }}
                className="px-4 py-2 bg-[#099c8f] text-white rounded-lg text-sm"
              >
                Enable Audio
              </button>
            </div>
          )}
        </div>
      </main>

      {/* Footer disclaimer */}
      <footer className="fixed bottom-0 left-0 right-0 bg-slate-50 border-t border-slate-200 py-3">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <p className="text-sm text-slate-600 text-center">
            This is a preliminary screening tool. For medical advice, consult a licensed physician.
          </p>
        </div>
      </footer>
    </div>
  );
}
