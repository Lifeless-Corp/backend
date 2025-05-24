from typing import Dict, Any, List, Optional, Union
import os
import json
import httpx
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class LLMResponse(BaseModel):
    """Model for LLM response"""
    text: str
    model_name: str
    tokens: Optional[Dict[str, int]] = None
    metadata: Optional[Dict[str, Any]] = None


class LLMRequest(BaseModel):
    """Model for LLM request"""
    prompt: str
    temperature: float = 1.0
    max_tokens: int = 500
    stream: bool = False


class LLMService:
    """Service for interacting with different LLM providers"""

    def __init__(self):
        # Ollama API
        self.ollama_api_url = os.getenv(
            "OLLAMA_API_URL", "http://localhost:11434/api")

        # Available models
        self.models = {
            "qwen:0.5b": {
                "provider": "ollama",
                "max_tokens": 2048,
                "description": "Qwen 0.5B - Small model for local use"
            }
        }

    async def list_models(self) -> List[Dict[str, Any]]:
        """Return list of available models with metadata"""
        return [{"id": model_id, **info} for model_id, info in self.models.items()]

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate text using the specified LLM"""
        """Generate text using Ollama API"""
        payload = {
            "model": "qwen:0.5b",
            "prompt": request.prompt,
            "options": {
                "temperature": request.temperature,
                "num_predict": request.max_tokens
            },
            "stream":  request.stream
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{self.ollama_api_url}/generate",
                json=payload
            )

            if response.status_code != 200:
                raise Exception(f"Error from Ollama API: {response.text}")

            result = response.json()

            return LLMResponse(
                text=result.get("response", ""),
                model_name="qwen:0.5b",
                tokens={
                    "prompt_tokens": result.get("prompt_eval_count", 0),
                    "completion_tokens": result.get("eval_count", 0),
                    "total_tokens": result.get("prompt_eval_count", 0) + result.get("eval_count", 0)
                },
                metadata={"provider": "ollama"}
            )


# Create a singleton instance
llm_service = LLMService()
