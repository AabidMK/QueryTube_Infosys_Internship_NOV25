import os
from dotenv import load_dotenv
from google import genai

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env")

client = genai.Client(api_key=API_KEY)

print("Fetching available models...\n")

try:
    models = client.models.list()
    
    print("=" * 60)
    print("AVAILABLE MODELS:")
    print("=" * 60)
    
    for model in models:
        print(f"\n📌 Model: {model.name}")
        if hasattr(model, 'supported_generation_methods'):
            print(f"   Supported methods: {model.supported_generation_methods}")
        if hasattr(model, 'display_name'):
            print(f"   Display name: {model.display_name}")
    
    print("\n" + "=" * 60)
    
except Exception as e:
    print(f"Error listing models: {e}")