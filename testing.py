import boto3
import os
import subprocess
import tempfile
import time
import json
import requests
from botocore.exceptions import ClientError
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS S3 bucket details
# SOURCE_BUCKET = input("Enter the source bucket name: ")
SOURCE_BUCKET = 'youtubespeechconvert'
# SOURCE_VIDEO = input("Enter the source video name: ")
SOURCE_VIDEO = 'OnTheJob Demo2.mp4'  # Adjust file extension if needed
TARGET_BUCKET = SOURCE_BUCKET  # Same bucket
# TARGET_VIDEO = input("Enter the target video name: ")
TARGET_VIDEO = 'OnTheJob Demo2 dutch.mp4'

# AWS credentials will be loaded from environment variables, AWS config, or instance role
s3_client = boto3.client('s3')
transcribe_client = boto3.client('transcribe')
translate_client = boto3.client('translate')
polly_client = boto3.client('polly')

def download_from_s3(bucket, key, local_path):
    """Download a file from S3 to local storage"""
    try:
        logger.info(f"Downloading {key} from S3 bucket {bucket}")
        s3_client.download_file(bucket, key, local_path)
        return True
    except ClientError as e:
        logger.error(f"Error downloading from S3: {e}")
        return False

def upload_to_s3(local_path, bucket, key):
    """Upload a file to S3 from local storage"""
    try:
        logger.info(f"Uploading to S3: {bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        return True
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False

def extract_audio(video_path, audio_path):
    """Extract audio from video file using FFmpeg"""
    try:
        logger.info(f"Extracting audio from video: {video_path} to {audio_path}")
        command = [
            'ffmpeg', '-i', video_path, 
            '-vn', '-acodec', 'pcm_s16le', 
            '-ar', '16000', '-ac', '1', 
            audio_path, '-y'
        ]
        subprocess.run(command, check=True)
        
        # Verify the file exists
        if os.path.exists(audio_path):
            logger.info(f"Audio extraction successful: {audio_path}")
            return True
        else:
            logger.error(f"Audio file not created: {audio_path}")
            return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Error extracting audio with FFmpeg: {e}")
        return False
    except FileNotFoundError:
        logger.error("FFmpeg not found. Please install FFmpeg and ensure it's in your PATH.")
        return False

def get_video_duration(video_path):
    """Get video duration using FFmpeg"""
    try:
        logger.info(f"Getting duration for video: {video_path}")
        command = [
            'ffmpeg', '-i', video_path, 
            '-f', 'null', '-'
        ]
        result = subprocess.run(command, stderr=subprocess.PIPE, text=True)
        
        # Extract duration from FFmpeg output
        for line in result.stderr.split('\n'):
            if 'Duration' in line:
                time_str = line.split('Duration: ')[1].split(',')[0]
                h, m, s = time_str.split(':')
                duration = float(h) * 3600 + float(m) * 60 + float(s)
                logger.info(f"Video duration: {duration} seconds")
                return duration
        
        logger.error("Could not determine video duration")
        return 60.0  # Default to 60 seconds if duration can't be determined
    except Exception as e:
        logger.error(f"Error getting video duration: {e}")
        return 60.0  # Default to 60 seconds if there's an error

def transcribe_audio(audio_path, job_name):
    """Transcribe audio using Amazon Transcribe"""
    try:
        # Check if audio file exists
        if not os.path.exists(audio_path):
            logger.error(f"Audio file not found: {audio_path}")
            return None, None
            
        logger.info(f"Transcribing audio: {audio_path}")
        
        # Upload audio to S3 for transcription
        audio_s3_key = f"temp/{job_name}.wav"
        if not upload_to_s3(audio_path, SOURCE_BUCKET, audio_s3_key):
            return None, None
        
        media_uri = f"s3://{SOURCE_BUCKET}/{audio_s3_key}"
        
        logger.info(f"Starting transcription job: {job_name}")
        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat='wav',
            LanguageCode='en-US'
        )
        
        # Wait for the transcription job to complete
        while True:
            status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            logger.info("Waiting for transcription to complete...")
            time.sleep(5)
        
        if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
            transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            logger.info(f"Transcription completed. Downloading transcript from: {transcript_uri}")
            
            # Download the transcript JSON
            response = requests.get(transcript_uri)
            transcript_data = response.json()
            
            # Extract the transcript text
            transcript_text = transcript_data['results']['transcripts'][0]['transcript']
            logger.info(f"Transcript text: {transcript_text[:100]}...")
            
            # Get the time-aligned words
            items = transcript_data['results']['items']
            
            return transcript_text, items
        else:
            logger.error("Transcription job failed")
            return None, None
            
    except Exception as e:
        logger.error(f"Error in transcription: {e}")
        return None, None

def translate_text(text, source_lang='en', target_lang='nl'):
    """Translate text from source language to target language"""
    try:
        logger.info(f"Translating text from {source_lang} to {target_lang}")
        
        # Split text into chunks if it's too long (Amazon Translate has a character limit)
        if len(text) > 5000:
            chunks = [text[i:i+5000] for i in range(0, len(text), 5000)]
            translated_chunks = []
            
            for chunk in chunks:
                response = translate_client.translate_text(
                    Text=chunk,
                    SourceLanguageCode=source_lang,
                    TargetLanguageCode=target_lang
                )
                translated_chunks.append(response['TranslatedText'])
            
            return ' '.join(translated_chunks)
        else:
            response = translate_client.translate_text(
                Text=text,
                SourceLanguageCode=source_lang,
                TargetLanguageCode=target_lang
            )
            return response['TranslatedText']
    except Exception as e:
        logger.error(f"Error translating text: {e}")
        return None

def synthesize_speech(text, output_path):
    """Synthesize speech using Amazon Polly"""
    try:
        logger.info(f"Synthesizing speech to {output_path}")
        
        # Clean up the text for better synthesis
        text = text.strip()
        if not text:
            logger.warning("Empty text provided for speech synthesis")
            return False
        
        # First, get available voices for Dutch
        available_voices = polly_client.describe_voices(LanguageCode='nl-NL')
        
        # Find a suitable female voice
        voice_id = None
        for voice in available_voices['Voices']:
            if voice['Gender'] == 'Female':
                voice_id = voice['Id']
                logger.info(f"Selected Dutch female voice: {voice_id}")
                break
        
        # If no Dutch female voice found, try a standard voice
        if not voice_id:
            voice_id = 'Lotte'  # Default to Lotte but with standard engine
            logger.warning(f"No suitable Dutch female voice found, using default: {voice_id}")
        
        # Amazon Polly has a character limit, so handle longer text
        if len(text) > 1500:
            # Split into chunks
            chunks = [text[i:i+1500] for i in range(0, len(text), 1500)]
            
            # Create a temporary file for each chunk
            temp_files = []
            for i, chunk in enumerate(chunks):
                temp_output = f"{output_path}.part{i}.mp3"
                
                # Synthesize each chunk
                response = polly_client.synthesize_speech(
                    Text=chunk,
                    OutputFormat='mp3',
                    VoiceId=voice_id,
                    # Use standard engine instead of neural
                    LanguageCode='nl-NL'
                )
                
                # Save the audio stream to a file
                with open(temp_output, 'wb') as file:
                    file.write(response['AudioStream'].read())
                
                temp_files.append(temp_output)
            
            # Combine the temporary files
            with open(output_path, 'wb') as outfile:
                for temp_file in temp_files:
                    with open(temp_file, 'rb') as infile:
                        outfile.write(infile.read())
            
            # Clean up temporary files
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            # For shorter text, synthesize directly
            response = polly_client.synthesize_speech(
                Text=text,
                OutputFormat='mp3',
                VoiceId=voice_id,
                # Use standard engine instead of neural
                LanguageCode='nl-NL'
            )
            
            # Save the audio stream to a file
            with open(output_path, 'wb') as file:
                file.write(response['AudioStream'].read())
        
        # Verify the file exists
        if os.path.exists(output_path):
            logger.info(f"Speech synthesis successful: {output_path}")
            return True
        else:
            logger.error(f"Speech file not created: {output_path}")
            return False
    except Exception as e:
        logger.error(f"Error synthesizing speech: {e}")
        return False
    
def simple_translate_and_synthesize(temp_dir):
    """Simplified approach: translate and synthesize the entire transcript at once"""
    try:
        logger.info("Using simplified approach for translation and synthesis")
        
        # Define file paths
        translated_text_path = os.path.join(temp_dir, "translated_text.txt")
        full_audio_path = os.path.join(temp_dir, "full_dutch_audio.mp3")
        
        # Read the extracted audio file
        audio_path = os.path.join(temp_dir, "extracted_audio.wav")
        
        # Transcribe the audio
        job_name = f"transcribe-job-{int(time.time())}"
        transcript_text, _ = transcribe_audio(audio_path, job_name)
        
        if not transcript_text:
            logger.error("Failed to transcribe audio")
            return None
        
        # Translate the transcript
        translated_text = translate_text(transcript_text)
        if not translated_text:
            logger.error("Failed to translate text")
            return None
            
        # Save translated text for reference
        with open(translated_text_path, 'w', encoding='utf-8') as f:
            f.write(translated_text)
        
        # Synthesize the full translated text
        if not synthesize_speech(translated_text, full_audio_path):
            logger.error("Failed to synthesize speech")
            return None
            
        return full_audio_path
    except Exception as e:
        logger.error(f"Error in simplified translation process: {e}")
        return None

def combine_audio_with_video(video_path, audio_path, output_path):
    """Combine audio with video using FFmpeg"""
    try:
        logger.info(f"Combining audio {audio_path} with video {video_path} to {output_path}")
        
        # Verify input files exist
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return False
        if not os.path.exists(audio_path):
            logger.error(f"Audio file not found: {audio_path}")
            return False
            
        command = [
            'ffmpeg',
            '-i', video_path,
            '-i', audio_path,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-map', '0:v:0',
            '-map', '1:a:0',
            '-shortest',
            output_path,
            '-y'
        ]
        subprocess.run(command, check=True)
        
        # Verify the output file exists
        if os.path.exists(output_path):
            logger.info(f"Successfully combined audio and video: {output_path}")
            return True
        else:
            logger.error(f"Output file not created: {output_path}")
            return False
    except Exception as e:
        logger.error(f"Error combining audio with video: {e}")
        return False

def main():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            logger.info(f"Created temporary directory: {temp_dir}")
            
            # Define file paths
            video_path = os.path.join(temp_dir, SOURCE_VIDEO)
            audio_path = os.path.join(temp_dir, "extracted_audio.wav")
            output_video_path = os.path.join(temp_dir, TARGET_VIDEO)
            
            # Step 1: Download the video from S3
            if not download_from_s3(SOURCE_BUCKET, SOURCE_VIDEO, video_path):
                logger.error("Failed to download video from S3")
                return
                
            # Verify the video file exists
            if not os.path.exists(video_path):
                logger.error(f"Downloaded video file not found: {video_path}")
                return
                
            # Step 2: Extract audio from the video
            if not extract_audio(video_path, audio_path):
                logger.error("Failed to extract audio from video")
                return
                
            # Step 3: Simplified approach - translate and synthesize the entire transcript
            dutch_audio_path = simple_translate_and_synthesize(temp_dir)
            if not dutch_audio_path:
                logger.error("Failed to generate Dutch audio")
                return
                
            # Step 4: Combine the Dutch audio with the original video
            if not combine_audio_with_video(video_path, dutch_audio_path, output_video_path):
                logger.error("Failed to combine audio with video")
                return
                
            # Step 5: Upload the result to S3
            if not upload_to_s3(output_video_path, TARGET_BUCKET, TARGET_VIDEO):
                logger.error("Failed to upload result to S3")
                return
                
            logger.info(f"Successfully processed video and uploaded to S3: {TARGET_BUCKET}/{TARGET_VIDEO}")
            
        except Exception as e:
            logger.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()