import os
from openai import OpenAI
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import duckdb   
import json
import tiktoken
from functools import lru_cache
from unthread_extractor.storage import DuckDBStorage

# Load environment variables from .env file
load_dotenv()

# Global cache for the system prompt
_RESOLUTION_PROMPT_CACHE: Optional[str] = None
_CATEGORY_PROMPT_CACHE: Optional[str] = None
_MODEL: str = "gpt-4o"
_STORAGE = DuckDBStorage("data/unthread_data.duckdb")

def get_system_prompt(type: str = "category") -> str:
    """Get the system prompt, cached for efficiency."""
    global _RESOLUTION_PROMPT_CACHE, _CATEGORY_PROMPT_CACHE
    
    if type == "category":
        if _CATEGORY_PROMPT_CACHE is None:
            prompt_path = os.path.join(os.path.dirname(__file__), "..", "..", "prompts", "reclassify.md")
            with open(prompt_path, "r") as file:
                _CATEGORY_PROMPT_CACHE = file.read()
        return _CATEGORY_PROMPT_CACHE
    elif type == "resolution":
        if _RESOLUTION_PROMPT_CACHE is None:
            prompt_path = os.path.join(os.path.dirname(__file__), "..", "..", "prompts", "resolution.md")
            with open(prompt_path, "r") as file:
                _RESOLUTION_PROMPT_CACHE = file.read()
        return _RESOLUTION_PROMPT_CACHE
    else:
        raise ValueError(f"Invalid type: {type}")

def generate_llm_response_batch(conversations: List[Dict[str, Any]], batch_size: int = 5) -> List[Dict[str, Any]]:
    """
    Generate responses for multiple conversations in batches to reduce API calls.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    client = OpenAI(api_key=api_key)
    system_prompt = get_system_prompt("resolution")
    
    results = []
    
    # Process conversations in batches
    for i in range(0, len(conversations), batch_size):
        batch = conversations[i:i + batch_size]
        
        # Create a batched prompt
        batched_prompt = "Please classify each of the following support cases:\n\n"
        for idx, conv in enumerate(batch, 1):
            batched_prompt += f"Case {idx}:\n<Conversation>{conv['message_content']}</Conversation>\n\n"
        
        batched_prompt += "Please respond with a JSON array containing the classification for each case in order."
        
        # Count tokens for monitoring
        print(f"Batch {i//batch_size + 1}: Processing {len(batch)} conversations")
        
        try:
            response = client.chat.completions.create(
                model=_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": batched_prompt}
                ]
            )
            
            result_text = response.choices[0].message.content
            
            # Try to parse as JSON array
            try:
                batch_results = json.loads(result_text)
                if isinstance(batch_results, list):
                    results.extend(batch_results)
                else:
                    # Fallback: treat as single result
                    results.append(batch_results)
            except json.JSONDecodeError:
                print(f"Error parsing batch response: {result_text}")
                # Add placeholder results for failed batch
                results.extend([{"error": "Failed to parse response"} for _ in batch])
                
        except Exception as e:
            print(f"Error processing batch: {e}")
            # Add placeholder results for failed batch
            results.extend([{"error": str(e)} for _ in batch])
    
    return results

def process_conversations_batch(conversations: List[Dict[str, Any]], batch_size: int = 5, max_conversations: Optional[int] = None):
    """
    Process conversations in batches for efficiency.
    """
    if max_conversations:
        conversations = conversations[:max_conversations]
    
    print(f"Processing {len(conversations)} conversations in batches of {batch_size}")
    
    # Process in batches
    results = generate_llm_response_batch(conversations, batch_size)
    
    # Save results to database
    _STORAGE.save_classifications(conversations, results)

if __name__ == "__main__":
    conversations = _STORAGE.get_conversations()
    
    # Use batch processing for efficiency
    process_conversations_batch(conversations, batch_size=10, max_conversations=100)