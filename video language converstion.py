import boto3
import pysrt
import os
import tempfile
from datetime import datetime, timedelta
import time
from pydub import AudioSegment
import urllib3
import logging
import botocore.config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_boto3_client(service_name):
    """Create a boto3 client with proper configuration to avoid SSL issues"""
    # Configure boto3 with appropriate retry settings and timeout settings
    config = botocore.config.Config(
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        },
        connect_timeout=60,
        read_timeout=60
    )
    
    # Create client with the custom config
    return boto3.client(
        service_name,
        config=config,
        # Use SigV4 signing which is more reliable
        # You can remove these lines if you're using AWS credentials from environment or ~/.aws/
        # use_ssl=True,
        # verify=True
    )

def download_srt_from_s3(bucket_name, srt_key, local_file_path):
    """Download SRT file from S3 bucket with error handling"""
    try:
        s3_client = create_boto3_client('s3')
        s3_client.download_file(bucket_name, srt_key, local_file_path)
        logger.info(f"Downloaded SRT file from s3://{bucket_name}/{srt_key} to {local_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading SRT file: {str(e)}")
        return False

def upload_to_s3(bucket_name, file_path, s3_key):
    """Upload a file to S3 bucket with error handling"""
    try:
        s3_client = create_boto3_client('s3')
        
        # Use ExtraArgs to ensure proper content type
        s3_client.upload_file(
            file_path, 
            bucket_name, 
            s3_key,
            ExtraArgs={'ContentType': 'audio/mpeg'}
        )
        logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return False

def translate_text(text, target_language='nl'):
    """Translate text to target language using Amazon Translate"""
    try:
        translate_client = create_boto3_client('translate')
        response = translate_client.translate_text(
            Text=text,
            SourceLanguageCode='auto',
            TargetLanguageCode=target_language
        )
        return response['TranslatedText']
    except Exception as e:
        logger.error(f"Error translating text: {str(e)}")
        # Return original text if translation fails
        return text

def generate_speech(text, output_file, voice_id='Lotte'):
    """Generate speech from text using Amazon Polly"""
    try:
        polly_client = create_boto3_client('polly')
        response = polly_client.synthesize_speech(
            Text=text,
            OutputFormat='mp3',
            VoiceId=voice_id  # 'Lotte' is a Dutch female voice
        )
        
        # Save the audio stream to a file
        with open(output_file, 'wb') as f:
            f.write(response['AudioStream'].read())
        
        return output_file
    except Exception as e:
        logger.error(f"Error generating speech: {str(e)}")
        return None

def milliseconds_to_time(milliseconds):
    """Convert milliseconds to time format used by pydub"""
    return milliseconds

def srt_to_dutch_speech(input_bucket, input_srt_key, output_bucket, output_audio_key):
    """
    Main function that:
    1. Downloads SRT from S3
    2. Translates subtitles to Dutch
    3. Generates speech for each subtitle
    4. Combines audio files respecting timing
    5. Uploads final audio to S3
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download SRT file
            srt_file_path = os.path.join(temp_dir, "input.srt")
            if not download_srt_from_s3(input_bucket, input_srt_key, srt_file_path):
                logger.error("Failed to download SRT file. Aborting.")
                return None
            
            # Parse SRT file
            try:
                subtitles = pysrt.open(srt_file_path)
            except Exception as e:
                logger.error(f"Error parsing SRT file: {str(e)}")
                return None
            
            # Create a blank audio segment
            if len(subtitles) > 0:
                # Get total duration from the last subtitle end time
                last_subtitle = subtitles[-1]
                total_duration_ms = last_subtitle.end.ordinal  # End time in milliseconds
                final_audio = AudioSegment.silent(duration=total_duration_ms)
            else:
                logger.error("No subtitles found in the SRT file")
                return None
            
            # Process each subtitle
            for idx, subtitle in enumerate(subtitles):
                subtitle_text = subtitle.text.replace("\n", " ")
                
                # Translate subtitle to Dutch
                dutch_text = translate_text(subtitle_text, 'nl')
                logger.info(f"Translated: '{subtitle_text}' -> '{dutch_text}'")
                
                # Generate speech for this subtitle
                audio_file = os.path.join(temp_dir, f"speech_{idx}.mp3")
                if not generate_speech(dutch_text, audio_file):
                    logger.warning(f"Failed to generate speech for subtitle {idx}. Skipping.")
                    continue
                
                # Load the generated audio
                try:
                    speech_segment = AudioSegment.from_mp3(audio_file)
                    
                    # Calculate timing
                    start_time_ms = subtitle.start.ordinal  # Start time in milliseconds
                    
                    # Overlay this audio at the correct timestamp
                    final_audio = final_audio.overlay(speech_segment, position=start_time_ms)
                except Exception as e:
                    logger.error(f"Error processing audio segment {idx}: {str(e)}")
                    continue
            
            # Save the final audio file
            output_file_path = os.path.join(temp_dir, "final_output.mp3")
            final_audio.export(output_file_path, format="mp3")
            
            # Upload to S3 - make sure we're using actual bucket names, not placeholders
            if not upload_to_s3(output_bucket, output_file_path, output_audio_key):
                logger.error("Failed to upload audio file to S3.")
                return None
            
            logger.info(f"Successfully processed SRT file and created Dutch speech audio")
            return f"s3://{output_bucket}/{output_audio_key}"
    except Exception as e:
        logger.error(f"Unexpected error in main processing function: {str(e)}")
        return None

if __name__ == "__main__":
    # Configuration - Replace these with your actual bucket names and paths
    INPUT_BUCKET = "youtubespeechconvert"  # Replace with your actual bucket name
    INPUT_SRT_KEY = "srtSubtitles.srt"  # Path to your SRT file in the bucket
    OUTPUT_BUCKET = INPUT_BUCKET  # Replace with your actual bucket name
    OUTPUT_AUDIO_KEY = "dutch_speech.mp3"  # Output path
    
    # Run the conversion process
    result = srt_to_dutch_speech(INPUT_BUCKET, INPUT_SRT_KEY, OUTPUT_BUCKET, OUTPUT_AUDIO_KEY)
    if result:
        logger.info(f"Audio file saved to: {result}")
    else:
        logger.error("Process failed. Check logs for details.")