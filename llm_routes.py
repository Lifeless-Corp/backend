from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Body
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from llm_service import llm_service, LLMRequest, LLMResponse

router = APIRouter(prefix="/llm", tags=["LLM"])


class DocumentQuery(BaseModel):
    """Model for querying documents with LLM assistance"""
    query: str = Field(..., description="User query text")
    documents: List[Dict[str, Any]
                    ] = Field(..., description="Documents to analyze")
    max_tokens: int = Field(500, description="Maximum tokens to generate")
    temperature: float = Field(0.7, description="Temperature for generation")


class LLMQueryResult(BaseModel):
    """Model for LLM query result"""
    query: str
    response: str
    model_used: str
    token_usage: Optional[Dict[str, int]] = None


@router.get("/models", response_model=List[Dict[str, Any]])
async def list_models():
    """List all available LLM models"""
    return await llm_service.list_models()


@router.post("/generate", response_model=LLMResponse)
async def generate_text(request: LLMRequest):
    """Generate text using the specified LLM"""
    try:
        return await llm_service.generate(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/llm/summarize")
async def summarize_top_articles(articles: list = Body(...)):
    """
    Summarize top articles using LLM.
    """
    summary = llm_service.summarize_articles(articles)
    return {"summary": summary}

@router.post("/analyze", response_model=LLMQueryResult)
async def analyze_documents(query: DocumentQuery):
    """Provide an overview of the user's query similar to Google Search Labs"""

    # Extract abstracts from documents to use as context
    document_contexts = []
    for i, doc in enumerate(query.documents):
        title = doc.get('title', f'Document {i+1}')
        abstract = doc.get('abstract', 'No abstract available')
        document_contexts.append(
            f"Document {i+1}: {title}\nAbstract: {abstract}")

    context = "\n\n".join(document_contexts)

    # Create prompt with document context
    prompt = f"""You are a helpful search assistant similar to Google Search Labs. 
    The user has entered the following query: "{query.query}"
    
    Here are the relevant documents:
    {context}
    
    Based on these documents and the query, please provide a concise, neutral overview of what this query is about.
    Focus on explaining the main topic, possible subtopics, and different aspects 
    that might be relevant. Your response should synthesize information from the documents
    to provide an informative introduction to the topic.
    """

    # Generate response
    request = LLMRequest(
        prompt=prompt,
        temperature=query.temperature,
        max_tokens=query.max_tokens
    )

    try:
        response = await llm_service.generate(request)

        return LLMQueryResult(
            query=query.query,
            response=response.text,
            model_used=response.model_name,
            token_usage=response.tokens
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
