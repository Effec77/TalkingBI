"""
Multi-Provider LLM Manager - Phase 0B Patch
Orchestrates 7 API keys across 4 providers with automatic fallback
"""
import os
import json
from typing import Optional, Dict, List
from dotenv import load_dotenv

load_dotenv()


class LLMManager:
    """
    Multi-provider LLM orchestrator with automatic fallback.
    
    Priority Order:
    1. Gemini (2 keys)
    2. Groq (2 keys) - fast fallback
    3. Mistral (2 keys) - secondary fallback
    4. OpenRouter (1 key) - last resort
    """
    
    def __init__(self):
        self.providers = [
            ("gemini", [
                os.getenv("GEMINI_API_KEY_1"),
                os.getenv("GEMINI_API_KEY_2")
            ]),
            ("groq", [
                os.getenv("GROQ_API_KEY_1"),
                os.getenv("GROQ_API_KEY_2")
            ]),
            ("mistral", [
                os.getenv("MISTRAL_API_KEY_1"),
                os.getenv("MISTRAL_API_KEY_2")
            ]),
            ("openrouter", [
                os.getenv("OPENROUTER_API_KEY")
            ])
        ]
        
        # Cache for responses
        self.cache = {}
    
    def call_llm(self, prompt: str, cache_key: Optional[str] = None) -> Optional[str]:
        """
        Call LLM with automatic provider fallback.
        
        Args:
            prompt: The prompt to send
            cache_key: Optional cache key
            
        Returns:
            LLM response or None if all providers fail
        """
        # Check cache
        if cache_key and cache_key in self.cache:
            print(f"[LLM] Cache hit for key: {cache_key}")
            return self.cache[cache_key]
        
        # Try each provider
        for provider_name, keys in self.providers:
            for i, key in enumerate(keys, 1):
                if not key:
                    continue
                
                try:
                    print(f"[LLM] Trying {provider_name} (key {i}/{len(keys)})")
                    response = self._call_provider(provider_name, key, prompt)
                    
                    if response:
                        print(f"[LLM] OK Success with {provider_name}")
                        
                        # Cache response
                        if cache_key:
                            self.cache[cache_key] = response
                        
                        return response
                        
                except Exception as e:
                    print(f"[LLM] ERROR {provider_name} key {i} failed: {str(e)[:100]}")
                    continue
        
        print("[LLM] WARN All providers failed -> returning None")
        return None
    
    def _call_provider(self, provider: str, key: str, prompt: str) -> Optional[str]:
        """Route to specific provider"""
        
        if provider == "gemini":
            return self._call_gemini(key, prompt)
        elif provider == "groq":
            return self._call_groq(key, prompt)
        elif provider == "mistral":
            return self._call_mistral(key, prompt)
        elif provider == "openrouter":
            return self._call_openrouter(key, prompt)
        else:
            raise ValueError(f"Unknown provider: {provider}")
    
    def _call_gemini(self, key: str, prompt: str) -> str:
        """Call Gemini API"""
        import google.generativeai as genai
        
        genai.configure(api_key=key)
        model = genai.GenerativeModel('gemini-pro-latest')
        response = model.generate_content(prompt)
        return response.text
    
    def _call_groq(self, key: str, prompt: str) -> str:
        """Call Groq API"""
        from groq import Groq
        
        client = Groq(api_key=key)
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=1024
        )
        return completion.choices[0].message.content
    
    def _call_mistral(self, key: str, prompt: str) -> str:
        """Call Mistral API"""
        from mistralai import Mistral
        
        client = Mistral(api_key=key)
        response = client.chat.complete(
            model="mistral-small-latest",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    
    def _call_openrouter(self, key: str, prompt: str) -> str:
        """Call OpenRouter API"""
        import requests
        
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "meta-llama/llama-3.1-8b-instruct:free",
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
