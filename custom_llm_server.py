from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import torch
from typing import Dict, Any, List, Optional
import os
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

app = FastAPI(title="Custom LLM API",
              description="API for serving local LLM models")

# Configuration
MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"  # Small model that can run on CPU


class GenerationRequest(BaseModel):
    """Model for generation request"""
    prompt: str = Field(..., description="The prompt text")
    model: str = Field("qwen", description="Model identifier")
    max_tokens: int = Field(500, description="Maximum tokens to generate")
    temperature: float = Field(0.7, description="Temperature for generation")


class GenerationResponse(BaseModel):
    """Model for generation response"""
    text: str
    model: str
    usage: Dict[str, int]


# Global variables for model and tokenizer
model = None
tokenizer = None


@app.on_event("startup")
async def startup_event():
    """Load model and tokenizer on startup"""
    global model, tokenizer

    print(f"Loading model {MODEL_ID}...")

    # Check for GPU availability (but use CPU for very small models)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    # Load model and tokenizer
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
            device_map=device
        )
        print("Model loaded successfully!")
    except Exception as e:
        print(f"Error loading model: {e}")
        # Don't fail startup, we'll check if model is loaded before inference


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Custom LLM API is running"}


@app.post("/generate", response_model=GenerationResponse)
async def generate(request: GenerationRequest):
    """Generate text from prompt"""
    global model, tokenizer

    if model is None or tokenizer is None:
        raise HTTPException(
            status_code=503, detail="Model not loaded. Please check server logs.")

    try:
        # Prepare inputs
        inputs = tokenizer(request.prompt, return_tensors="pt")
        input_tokens = inputs.input_ids.shape[1]

        # Move to device (if using GPU)
        if torch.cuda.is_available():
            inputs = {k: v.to("cuda") for k, v in inputs.items()}

        # Generate
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=request.max_tokens,
                temperature=request.temperature,
                do_sample=request.temperature > 0,
                pad_token_id=tokenizer.eos_token_id
            )

        # Decode
        output_text = tokenizer.decode(outputs[0], skip_special_tokens=True)

        # Some models include the prompt in the output, remove it if needed
        if output_text.startswith(request.prompt):
            output_text = output_text[len(request.prompt):].strip()

        # Count output tokens
        output_tokens = outputs.shape[1] - input_tokens

        return GenerationResponse(
            text=output_text,
            model=MODEL_ID,
            usage={
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error during generation: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
