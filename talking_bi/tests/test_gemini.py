"""
Test Gemini API connection
"""
import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

print(f"API Key: {GEMINI_API_KEY[:20]}...")

try:
    import google.generativeai as genai
    
    genai.configure(api_key=GEMINI_API_KEY)
    
    # List available models
    print("\nAvailable models:")
    for model in genai.list_models():
        if 'generateContent' in model.supported_generation_methods:
            print(f"  - {model.name}")
    
    # Try a simple generation
    print("\nTesting generation with gemini-pro...")
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content("Say hello")
    print(f"Response: {response.text}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
