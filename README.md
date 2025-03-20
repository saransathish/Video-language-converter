# Video Language Converter Using AWS Services

This project allows users to convert the spoken language in a video to another language using AWS services. The system performs the following steps:

1. Uploads the video to an AWS S3 bucket.
2. Uses AWS Transcribe to extract speech and convert it into text.
3. Translates the extracted text using AWS Translate.
4. Converts the translated text into speech using AWS Polly.

## Prerequisites

- AWS Account
- AWS CLI Installed and Configured
- IAM Role with necessary permissions
- Python Installed (>= 3.7)
- Boto3 Library (AWS SDK for Python)

## AWS Setup

1. **Configure AWS CLI**
   ```sh
   aws configure
   ```
   Provide your AWS Access Key, Secret Key, Region, and Output format.

2. **Create an S3 Bucket**
   ```sh
   aws s3 mb s3://your-bucket-name
   ```

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/video-language-converter.git
   cd video-language-converter
   ```

2. Install required dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Usage

### Upload Video to S3
```sh
aws s3 cp path/to/your/video.mp4 s3://your-bucket-name/
```

### Run the Script
Create a `config.py` file with your S3 bucket name and video path:
```python
S3_BUCKET = "your-bucket-name"
S3_VIDEO_PATH = "your-video.mp4"
TARGET_LANGUAGE = "es"  # Change to desired target language
```

Run the script:
```sh
python main.py
```

## Output
- The script will generate a translated file into same s3 bucket.
- Logs will display the transcription URL, translated text, and speech synthesis status.

## License
This project is licensed under the MIT License.

---
Feel free to modify the `TARGET_LANGUAGE` variable in `config.py` to change the target translation language.

### Contributions
Pull requests are welcome! Please ensure your changes are well-documented.

---
Happy coding! 

