import os
import json
import io
import asyncio
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

from PIL import Image

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.services.llm_service import FunctionCallParams
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
)
from pipecat.frames.frames import (
    LLMMessagesAppendFrame,
    InputImageRawFrame,
    StartFrame,
    FunctionCallResultProperties,
)
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

import sys

sys.path.insert(0, "/home/sach/GEMINI_LIVE/server")
from tb_audio_tool import analyze_cough_file, AudioCapture, save_audio_to_wav
from palm_anemia_tool import analyze_palm_file
from eye_anemia_tool import analyze_eye_file
from nail_anemia_tool import analyze_nail_file
from chest_xray_tool import analyze_xray_file
from xray_store import get_latest_xray_path

logging.basicConfig(level=logging.INFO)

# Suppress aiortc VP8 decoder warnings (common WebRTC video codec issue)
logging.getLogger("aiortc.codecs.vpx").setLevel(logging.ERROR)
load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY", "")

audio_capture = AudioCapture()
is_recording_cough = False
audio_buffer = None  # Will be set after creation
is_recording_cough_stream = False
cough_stream_buffer = bytearray()
cough_stream_sample_rate = 24000
last_cough_record_end_ts = 0.0
awaiting_cough_after_prompt = False
bot_is_speaking = False
got_user_track_audio = False
latest_image_frame = None
last_cough_file_path = None
last_palm_file_path = None
is_xray_analysis_running = False
is_eye_analysis_running = False
completed_checks = {"palm": False, "eye": False, "nail": False, "cough": False}


def next_pending_check():
    for key in ["palm", "eye", "nail"]:
        if not completed_checks.get(key, False):
            return key
    return None


async def prompt_next_check(task: PipelineTask):
    pending = next_pending_check()
    if not pending:
        return
    prompts = {
        "palm": "Would you like to check your palm for anemia? If yes, please show your palm to the camera.",
        "eye": "Would you like to check your eye for anemia? If yes, come closer and gently pull down your lower eyelid.",
        "nail": "Would you like to check your fingernail for anemia? If yes, show one nail close to the camera.",
    }
    await task.queue_frames(
        [
            LLMMessagesAppendFrame(
                messages=[
                    {
                        "role": "user",
                        "content": prompts[pending],
                    }
                ]
            )
        ]
    )


def save_analysis_json(source_path: str, analysis: dict) -> str:
    """Save analysis JSON alongside the source file and return the path."""
    base, _ = os.path.splitext(source_path)
    analysis_path = f"{base}_analysis.json"
    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)
    return analysis_path


class LatestImageCaptureProcessor(FrameProcessor):
    """Keeps the most recent input image frame in memory."""

    def __init__(self):
        super().__init__()
        self._started = False

    async def process_frame(self, frame, direction: FrameDirection):
        global latest_image_frame
        if isinstance(frame, StartFrame):
            self._started = True
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)
            return

        if not self._started:
            return

        if isinstance(frame, InputImageRawFrame):
            latest_image_frame = frame
        await self.push_frame(frame, direction)


class UserTurnStartProcessor(FrameProcessor):
    """Starts cough recording window when the user begins speaking."""

    def __init__(self, start_cough_callback):
        super().__init__()
        self._start_cough_callback = start_cough_callback
        self._started = False

    async def process_frame(self, frame, direction: FrameDirection):
        from pipecat.frames.frames import UserStartedSpeakingFrame
        from pipecat.frames.frames import StartFrame

        if isinstance(frame, StartFrame):
            self._started = True
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)
            return

        if not self._started:
            return

        if isinstance(frame, UserStartedSpeakingFrame):
            if awaiting_cough_after_prompt and not is_recording_cough:
                # Start cough window as soon as user begins speaking
                asyncio.create_task(self._start_cough_callback())
        await self.push_frame(frame, direction)


def get_record_tool() -> FunctionSchema:
    """Tool to record user's cough sound for TB analysis."""
    return FunctionSchema(
        name="record_cough_sound",
        description="Records the user's cough sound. Call this tool when user wants TB analysis. It will capture 4 seconds of audio. Returns a file path. Use this BEFORE analyze_cough_for_tb.",
        properties={},
        required=[],
    )


def get_analyze_tool() -> FunctionSchema:
    """Tool to analyze recorded cough for TB probability."""
    return FunctionSchema(
        name="analyze_cough_for_tb",
        description="Analyzes the recorded cough for TB probability. Call this AFTER record_cough_sound. No arguments needed - it automatically analyzes the captured recording. Returns TB probability and interpretation.",
        properties={},
        required=[],
    )


def get_capture_palm_tool() -> FunctionSchema:
    """Tool to capture the latest palm photo from the camera."""
    return FunctionSchema(
        name="capture_palm_photo",
        description="Captures the current camera frame and saves it locally after a palm is clearly visible.",
        properties={
            "label": {
                "type": "string",
                "description": "Optional filename label for the saved image.",
            }
        },
        required=[],
    )


def get_capture_eye_tool() -> FunctionSchema:
    """Tool to capture the latest eye photo from the camera."""
    return FunctionSchema(
        name="capture_eye_photo",
        description="Captures the current camera frame and saves it locally after the eye is clearly visible.",
        properties={
            "label": {
                "type": "string",
                "description": "Optional filename label for the saved image.",
            }
        },
        required=[],
    )


def get_capture_fingernail_tool() -> FunctionSchema:
    """Tool to capture the latest fingernail photo from the camera."""
    return FunctionSchema(
        name="capture_fingernail_photo",
        description="Captures the current camera frame and saves it locally after a fingernail is clearly visible and close to the camera.",
        properties={
            "label": {
                "type": "string",
                "description": "Optional filename label for the saved image.",
            }
        },
        required=[],
    )


def get_analyze_xray_tool() -> FunctionSchema:
    """Tool to analyze the most recently uploaded chest X-ray."""
    return FunctionSchema(
        name="analyze_chest_xray",
        description="Analyzes the most recently uploaded chest X-ray image for TB indicators.",
        properties={},
        required=[],
    )


transport_params = {
    "daily": lambda: DailyParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        video_in_enabled=True,
        camera_out_enabled=True,
    ),
    "webrtc": lambda: TransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        video_in_enabled=True,
        camera_out_enabled=True,
    ),
}


async def run_bot(transport: BaseTransport, runner_args):
    from pipecat.runner.utils import (
        maybe_capture_participant_camera,
        maybe_capture_participant_screen,
    )
    from pipecat.frames.frames import LLMMessagesAppendFrame

    print(f"Transport: {transport}")

    if not api_key:
        print("ERROR: No GOOGLE_API_KEY found!")
        return

    print(f"Using API key: {api_key[:20]}...")

    try:
        record_tool = get_record_tool()
        analyze_tool = get_analyze_tool()
        capture_palm_tool = get_capture_palm_tool()
        capture_eye_tool = get_capture_eye_tool()
        capture_fingernail_tool = get_capture_fingernail_tool()
        analyze_xray_tool = get_analyze_xray_tool()

        tools_schema = ToolsSchema(
            standard_tools=[
                record_tool,
                analyze_tool,
                capture_palm_tool,
                capture_eye_tool,
                capture_fingernail_tool,
                analyze_xray_tool,
            ]
        )

        llm = GeminiLiveLLMService(
            api_key=api_key,
            model="gemini-2.5-flash-native-audio-preview-12-2025",
            voice_id="Charon",
            system_instruction="""You are BigTBAI, a friendly and professional health companion for the Gemini Live Medical Diagnostics platform.

Your main goal is to have a natural, empathetic conversation about the user's health concerns and guide them toward appropriate care. This is a comprehensive screening platform that analyzes multiple health indicators—including cough sounds for TB assessment and eye/palm/nail images for anemia detection—providing a thorough health evaluation experience.

CONVERSATION STYLE:
- Be warm, friendly, and conversational
- Show genuine concern for the user's wellbeing
- Listen actively to what the user says
- Respond naturally and ask thoughtful follow-up questions
- Keep responses concise and easy to understand

HEALTH ASSESSMENT APPROACH:
- Ask about specific symptoms: "How long have you been experiencing this?", "Is it getting better or worse?"
- Show empathy: "That sounds concerning", "I understand how worrying this must be"
- Gather context: "Have you had any fever?", "Are you experiencing any other symptoms?"
- After the cough discussion, explain that the platform also checks eye, palm, and fingernail appearance for anemia indicators, and guide the user to show those body parts on camera if anything seems unusual.

ABOUT TB ANALYSIS:
- You can help assess TB risk by analyzing cough sounds using AI-powered audio analysis
- When users mention cough, I have cough , I have cough for some time or few days or  someday, fever, analyze my cough ,weight loss, or night sweats, you can mention: "I can analyze your cough to help assess TB risk if you'd like"- Then ask: "Are you ready to start?"
- Only start recording after explicit readiness from the user
- Accept readiness phrases like: "I'm ready", "yes", "go on", "grab it", "ready to cough", "start", "ok", "sure", "do it"
- If you asked "Are you ready to start?" and the user answers "yes", treat it as readiness and call record_cough_sound immediately

PROACTIVE ANEMIA SCREENING:
- ACTIVELY ask users about fatigue, weakness, dizziness, or paleness
- If they mention ANY of these symptoms, offer anemia screening: "Those symptoms could indicate anemia. I can analyze your eye, palm, or fingernail images to check for anemia indicators. Would you like me to do that?"
- Use these EXACT trigger phrases when users want analysis:
  * "analyze eye for anemia" - for eye analysis
  * "analyze palm for anemia" - for palm analysis
  * "analyze nail for anemia" - for nail analysis
  * "comprehensive anemia screening" - for all three analyses

INTEGRATED SCREENING APPROACH:
- Explain that the platform provides comprehensive screening by checking multiple health indicators
- After cough analysis, suggest anemia screening as a complementary health check
- Mention that combining results from different analyses gives a more complete health picture
- Use comprehensive analysis when the user wants a complete health screening

TOOL RULES:
- When the user says they are ready (e.g., "I'm ready", "yes", "go on", "grab it", "ready to cough", "start", "ok", "sure", "do it"), call record_cough_sound immediately
- After record_cough_sound returns, tell user to cough and wait ~5 seconds
- Do NOT call analyze_cough_for_tb until you receive a message saying the recording is complete
- If the palm is visible and the user says they are showing it, call capture_palm_photo immediately
- If the palm looks aligned in frame, capture it
- If the eye is visible and the user says they are showing it, call capture_eye_photo immediately
- If the fingernail is visible and the user says they are showing it, call capture_fingernail_photo immediately
- If the user confirms X-ray upload, call analyze_chest_xray immediately and do not speak before calling it

IMPORTANT:
- You are NOT a doctor—always recommend professional medical consultation
- Never prescribe medication or give definitive diagnoses
- Your output will be spoken aloud, so avoid special characters and emojis
- Always end with supportive guidance: "Please consult with a healthcare professional for proper diagnosis and treatment"
- Explain that all analyses use AI technology and have limitations—professional medical diagnosis is essential
""",
            tools=tools_schema,
            function_call_timeout_secs=180.0,
        )
        print("LLM service created")

        async def handle_tool_calls(params: FunctionCallParams):
            """Handle tool calls from Gemini Live."""
            global is_recording_cough, audio_buffer

            function_name = params.function_name
            args = params.arguments

            if function_name == "record_cough_sound":
                global last_cough_file_path
                global is_recording_cough_stream
                global cough_stream_buffer
                global last_cough_record_end_ts
                global awaiting_cough_after_prompt

                # Ignore if already recording
                if is_recording_cough:
                    props = FunctionCallResultProperties(run_llm=False)
                    await params.result_callback(
                        {"status": "recording_in_progress"},
                        properties=props,
                    )
                    return

                # Cooldown to prevent retrigger
                now_ts = datetime.now().timestamp()
                if now_ts - last_cough_record_end_ts < 5:
                    props = FunctionCallResultProperties(run_llm=False)
                    await params.result_callback(
                        {"status": "cooldown_active"},
                        properties=props,
                    )
                    return

                # Start recording window shortly after the bot speaks the prompt
                print("Starting cough recording - asking user to cough...")
                audio_capture.clear()
                awaiting_cough_after_prompt = True
                got_user_track_audio = False
                got_user_track_audio = False

                # Wait for user speech start to begin recording (handled by UserTurnStartProcessor)

                # AudioBufferProcessor is started on client connect and runs continuously.

                # Let the LLM speak the cough instruction as its response

                # Allow the LLM to respond immediately with instructions
                props = FunctionCallResultProperties(run_llm=True)
                await params.result_callback(
                    {"status": "recording_started"},
                    properties=props,
                )
                return

            elif function_name == "analyze_cough_for_tb":
                # Stop recording if still recording
                is_recording_cough = False

                # Give a moment for final audio to be captured
                await asyncio.sleep(0.5)

                if last_cough_file_path and os.path.exists(last_cough_file_path):
                    if os.path.getsize(last_cough_file_path) <= 44:
                        await params.result_callback(
                            {
                                "error": "No audio recorded. Please try again and cough clearly."
                            }
                        )
                        return
                    file_path = last_cough_file_path
                else:
                    capture_dir = os.path.join(
                        os.path.dirname(__file__), "cough_samples"
                    )
                    os.makedirs(capture_dir, exist_ok=True)
                    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    file_path = os.path.join(capture_dir, f"cough_{timestamp}.wav")
                    audio_data = audio_capture.audio_data
                    print(f"📊 Audio data size: {len(audio_data)} bytes")
                    if len(audio_data) == 0:
                        await params.result_callback(
                            {
                                "error": "No audio recorded. Please try again and cough clearly."
                            }
                        )
                        return
                    file_path = save_audio_to_wav(audio_data, 24000, file_path)

                print(f"💾 Cough saved to: {file_path}")

                print(f"🔍 Analyzing cough from: {file_path}")
                result = await analyze_cough_file(file_path)
                print(f"📋 Analysis result: {result}")

                analysis_path = save_analysis_json(file_path, result)
                await params.result_callback(
                    {**result, "analysis_path": analysis_path}
                )
                if not result.get("error"):
                    completed_checks["cough"] = True
                    await prompt_next_check(task)
                return
            if function_name == "capture_palm_photo":
                global last_palm_file_path
                if latest_image_frame is None:
                    await params.result_callback(
                        {
                            "error": "No camera frame available. Please show your palm to the camera."
                        }
                    )
                    return

                args = args or {}
                label = args.get("label") or "palm"
                safe_label = "".join(
                    c if c.isalnum() or c in "-_" else "_" for c in label
                )

                capture_dir = os.path.join(
                    os.path.dirname(__file__), "palm_captures"
                )
                os.makedirs(capture_dir, exist_ok=True)

                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"{safe_label}_{timestamp}.png"
                file_path = os.path.join(capture_dir, filename)

                image_format = latest_image_frame.format or "RGB"
                try:
                    if image_format.startswith("image/"):
                        image = Image.open(io.BytesIO(latest_image_frame.image))
                    else:
                        image = Image.frombytes(
                            image_format,
                            latest_image_frame.size,
                            latest_image_frame.image,
                        )
                    image.save(file_path, format="PNG")
                except Exception as exc:
                    await params.result_callback(
                        {"error": f"Failed to save image: {exc}"}
                    )
                    return

                last_palm_file_path = file_path
                print(f"Calling palm anemia API for: {file_path}")
                result = await analyze_palm_file(file_path)
                print(f"Palm analysis result: {result}")
                analysis_path = save_analysis_json(file_path, result)
                if not result.get("error"):
                    completed_checks["palm"] = True
                await task.queue_frames(
                    [
                        LLMMessagesAppendFrame(
                            messages=[
                                {
                                    "role": "user",
                                    "content": "Photo captured. Analyzing now. I will report the result soon.",
                                }
                            ]
                        )
                    ]
                )
                await params.result_callback(
                    {
                        "status": "ok",
                        "path": file_path,
                        "analysis": result,
                        "analysis_path": analysis_path,
                    }
                )
                if not result.get("error"):
                    await prompt_next_check(task)
                return
            elif function_name == "capture_eye_photo":
                global is_eye_analysis_running
                if latest_image_frame is None:
                    await params.result_callback(
                        {
                            "error": "I can’t see your eye clearly. Please come closer, remove any glasses, and gently pull down the lower eyelid so it fills most of the frame."
                        }
                    )
                    return

                args = args or {}
                label = args.get("label") or "eye"
                safe_label = "".join(
                    c if c.isalnum() or c in "-_" else "_" for c in label
                )

                capture_dir = os.path.join(
                    os.path.dirname(__file__), "eye_images"
                )
                os.makedirs(capture_dir, exist_ok=True)

                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"{safe_label}_{timestamp}.png"
                file_path = os.path.join(capture_dir, filename)

                image_format = latest_image_frame.format or "RGB"
                try:
                    if image_format.startswith("image/"):
                        image = Image.open(io.BytesIO(latest_image_frame.image))
                    else:
                        image = Image.frombytes(
                            image_format,
                            latest_image_frame.size,
                            latest_image_frame.image,
                        )
                    image.save(file_path, format="PNG")
                except Exception as exc:
                    await params.result_callback(
                        {"error": f"Failed to save image: {exc}"}
                    )
                    return

                print(f"Calling eye analysis API for: {file_path}")
                result = await analyze_eye_file(file_path)
                print(f"Eye analysis result: {result}")
                analysis_path = save_analysis_json(file_path, result)
                if not result.get("error"):
                    completed_checks["eye"] = True

                await params.result_callback(
                    {
                        "status": "ok",
                        "path": file_path,
                        "analysis": result,
                        "analysis_path": analysis_path,
                        "message": "Photo captured. Analyzing now.",
                    }
                )
                if not result.get("error"):
                    await prompt_next_check(task)
                return
            elif function_name == "capture_fingernail_photo":
                if latest_image_frame is None:
                    await params.result_callback(
                        {
                            "error": "No camera frame available. Please show your fingernail to the camera."
                        }
                    )
                    return

                args = args or {}
                label = args.get("label") or "fingernail"
                safe_label = "".join(
                    c if c.isalnum() or c in "-_" else "_" for c in label
                )

                capture_dir = os.path.join(
                    os.path.dirname(__file__), "fingernail_images"
                )
                os.makedirs(capture_dir, exist_ok=True)

                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"{safe_label}_{timestamp}.png"
                file_path = os.path.join(capture_dir, filename)

                image_format = latest_image_frame.format or "RGB"
                try:
                    if image_format.startswith("image/"):
                        image = Image.open(io.BytesIO(latest_image_frame.image))
                    else:
                        image = Image.frombytes(
                            image_format,
                            latest_image_frame.size,
                            latest_image_frame.image,
                        )
                    image.save(file_path, format="PNG")
                except Exception as exc:
                    await params.result_callback(
                        {"error": f"Failed to save image: {exc}"}
                    )
                    return

                print(f"Calling nail analysis API for: {file_path}")
                result = await analyze_nail_file(file_path)
                print(f"Nail analysis result: {result}")
                analysis_path = save_analysis_json(file_path, result)
                if not result.get("error"):
                    completed_checks["nail"] = True
                await params.result_callback(
                    {
                        "status": "ok",
                        "path": file_path,
                        "analysis": result,
                        "analysis_path": analysis_path,
                    }
                )
                if not result.get("error"):
                    await prompt_next_check(task)
                return
            elif function_name == "analyze_chest_xray":
                global is_xray_analysis_running
                latest_path = get_latest_xray_path()
                if not latest_path or not os.path.exists(latest_path):
                    await params.result_callback(
                        {
                            "error": "No chest X-ray found. Please upload an X-ray image first."
                        }
                    )
                    return
                if is_xray_analysis_running:
                    await params.result_callback(
                        {"status": "processing", "message": "Chest X-ray analysis is already in progress."}
                    )
                    return

                is_xray_analysis_running = True

                # Return immediately without triggering LLM response
                props = FunctionCallResultProperties(run_llm=False)
                await params.result_callback(
                    {
                        "status": "processing",
                        "message": "Chest X-ray analysis has started. Results will be provided when ready.",
                    },
                    properties=props,
                )

                async def run_xray_analysis():
                    global is_xray_analysis_running
                    try:
                        print(f"Calling chest X-ray API for: {latest_path}")
                        result = await analyze_xray_file(latest_path)
                        print(f"Chest X-ray analysis result: {result}")
                        analysis_path = save_analysis_json(latest_path, result)

                        await task.queue_frames(
                            [
                                LLMMessagesAppendFrame(
                                    messages=[
                                        {
                                            "role": "user",
                                            "content": (
                                                "Chest X-ray result JSON: "
                                                + json.dumps(
                                                    {
                                                        "analysis": result,
                                                        "analysis_path": analysis_path,
                                                    },
                                                    ensure_ascii=False,
                                                )
                                                + ". Use only this data."
                                            ),
                                        }
                                    ]
                                )
                            ]
                        )
                    finally:
                        is_xray_analysis_running = False

                asyncio.create_task(run_xray_analysis())
                return

            await params.result_callback(
                {"error": f"Unknown function: {function_name}"}
            )
            return

        # Create context for tool calls (universal context + aggregators)
        from pipecat.processors.aggregators.llm_context import LLMContext
        from pipecat.processors.aggregators.llm_response_universal import (
            LLMContextAggregatorPair,
        )

        context = LLMContext([], tools=tools_schema)
        context_aggregator = LLMContextAggregatorPair(context)
        room_url = os.getenv("DAILY_ROOM_URL", "")

        @context_aggregator.assistant().event_handler("on_assistant_turn_stopped")
        async def on_assistant_turn_stopped(aggregator, message):
            text = getattr(message, "content", "") or ""
            text = text.strip()
            if not text:
                return
            payload = {"text": text, "room_url": room_url}
            print(f"BOT_TEXT:{json.dumps(payload, ensure_ascii=True)}")

        # Register function handlers after context is set
        llm.register_function(
            "record_cough_sound",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(
            "analyze_cough_for_tb",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(
            "capture_palm_photo",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(
            "capture_eye_photo",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(
            "capture_fingernail_photo",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(
            "analyze_chest_xray",
            handle_tool_calls,
            cancel_on_interruption=False,
        )
        llm.register_function(None, handle_tool_calls)

        audio_buffer = AudioBufferProcessor(
            enable_turn_audio=True,
            num_channels=1,
            sample_rate=24000,
            user_continuous_stream=True,
            # 1-second chunks to ensure callbacks fire during short recordings
            buffer_size=24000 * 2 * 1,
        )

        @audio_buffer.event_handler("on_user_turn_audio_data")
        async def on_user_audio(buffer, audio_bytes: bytes, sample_rate: int, channels: int):
            global is_recording_cough
            if is_recording_cough:
                audio_capture.add_audio(audio_bytes)
                print(
                    f"🎤 Audio captured: {len(audio_bytes)} bytes (total: {len(audio_capture.audio_data)} bytes)"
                )

        @audio_buffer.event_handler("on_audio_data")
        async def on_audio_data(buffer, audio, sample_rate, num_channels):
            global cough_stream_buffer, cough_stream_sample_rate
            if is_recording_cough_stream and audio and not got_user_track_audio:
                cough_stream_sample_rate = sample_rate
                cough_stream_buffer.extend(audio)

        @audio_buffer.event_handler("on_track_audio_data")
        async def on_track_audio_data(
            buffer, user_audio, bot_audio, sample_rate, num_channels
        ):
            global cough_stream_buffer, cough_stream_sample_rate, got_user_track_audio
            if is_recording_cough_stream and user_audio:
                got_user_track_audio = True
                cough_stream_sample_rate = sample_rate
                cough_stream_buffer.extend(user_audio)

        latest_image_capture = LatestImageCaptureProcessor()

        async def start_cough_recording_window():
            global is_recording_cough, is_recording_cough_stream, cough_stream_buffer
            global awaiting_cough_after_prompt, got_user_track_audio
            is_recording_cough = True
            is_recording_cough_stream = True
            cough_stream_buffer = bytearray()
            got_user_track_audio = False
            awaiting_cough_after_prompt = False
            asyncio.create_task(stop_recording_and_analyze())

        async def stop_recording_and_analyze():
            global is_recording_cough, is_recording_cough_stream, last_cough_record_end_ts
            global last_cough_file_path

            print("🔴 Recording started - waiting 5 seconds for cough...")
            await asyncio.sleep(5)

            # Do not stop the audio buffer; keep it running continuously.

            is_recording_cough = False
            is_recording_cough_stream = False
            last_cough_record_end_ts = datetime.now().timestamp()
            print("✅ Recording stopped - saving cough sample...")

            capture_dir = os.path.join(os.path.dirname(__file__), "cough_samples")
            os.makedirs(capture_dir, exist_ok=True)
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(capture_dir, f"cough_{timestamp}.wav")
            if len(cough_stream_buffer) > 0:
                last_cough_file_path = save_audio_to_wav(
                    bytes(cough_stream_buffer),
                    cough_stream_sample_rate,
                    file_path,
                )
            else:
                last_cough_file_path = save_audio_to_wav(
                    audio_capture.audio_data, 24000, file_path
                )

            # Inject message to trigger the next tool call
            await task.queue_frames(
                [
                    LLMMessagesAppendFrame(
                        messages=[
                            {
                                "role": "user",
                                "content": "Recording complete. The cough has been captured. Please now call the analyze_cough_for_tb tool to analyze this recording for TB probability.",
                            }
                        ]
                    )
                ]
            )

        user_turn_start = UserTurnStartProcessor(start_cough_recording_window)

        pipeline = Pipeline(
            [
                transport.input(),
                latest_image_capture,
                user_turn_start,
                context_aggregator.user(),
                llm,
                transport.output(),
                audio_buffer,
                context_aggregator.assistant(),
            ]
        )
        # Disable Pipecat's default idle timeout (300s) so the session relies
        # on explicit disconnect/room expiration instead of auto-cancel.
        try:
            task = PipelineTask(
                pipeline,
                params=PipelineParams(enable_metrics=True),
                idle_timeout_secs=None,
                cancel_on_idle_timeout=False,
            )
        except TypeError:
            # Fallback for older PipelineTask signatures.
            task = PipelineTask(pipeline, params=PipelineParams(enable_metrics=True))
        print("Running task...")
        client_connected = asyncio.Event()
        shutdown_task = None

        async def _shutdown_if_no_client(timeout_seconds: int = 60):
            try:
                await asyncio.wait_for(client_connected.wait(), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                print("No client joined within timeout. Shutting down bot.")
                await task.cancel()

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            print(f"Client connected: {client}")
            client_connected.set()
            await maybe_capture_participant_camera(transport, client, framerate=1)
            await maybe_capture_participant_screen(transport, client, framerate=1)
            llm.set_video_input_paused(False)
            if audio_buffer:
                await audio_buffer.start_recording()
            await task.queue_frames(
                [
                    LLMMessagesAppendFrame(
                        messages=[
                            {
                                "role": "user",
                                "content": "Say exactly this to the user: Hello! I'm BigTB6, your personal health companion. I'm here to help you assess your health today. How are you feeling?",
                            }
                        ]
                    )
                ]
            )

        @transport.event_handler("on_bot_stopped_speaking")
        async def on_bot_stopped_speaking(transport, client):
            global bot_is_speaking
            bot_is_speaking = False

        @transport.event_handler("on_bot_started_speaking")
        async def on_bot_started_speaking(transport, client):
            global bot_is_speaking
            bot_is_speaking = True

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            print(f"Client disconnected")
            await task.cancel()

        runner = PipelineRunner()
        shutdown_task = asyncio.create_task(_shutdown_if_no_client())
        await runner.run(task)
        if shutdown_task:
            shutdown_task.cancel()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()


async def bot(runner_args):
    from pipecat.runner.utils import create_transport

    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport, runner_args)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
