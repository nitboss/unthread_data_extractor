import os
from openai import OpenAI
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import duckdb   
import json
import tiktoken
from functools import lru_cache

# Load environment variables from .env file
load_dotenv()

# Global cache for the system prompt
_SYSTEM_PROMPT_CACHE: Optional[str] = None
_OPTIMIZED_PROMPT_CACHE: Optional[str] = None
_MODEL: str = "gpt-3.5-turbo"

def get_system_prompt(use_optimized: bool = True) -> str:
    """Get the system prompt, cached for efficiency."""
    global _SYSTEM_PROMPT_CACHE, _OPTIMIZED_PROMPT_CACHE
    
    if use_optimized:
        if _OPTIMIZED_PROMPT_CACHE is None:
            prompt_path = os.path.join(os.path.dirname(__file__), "..", "..", "prompts", "reclassify_optimized.md")
            with open(prompt_path, "r") as file:
                _OPTIMIZED_PROMPT_CACHE = file.read()
        return _OPTIMIZED_PROMPT_CACHE
    else:
        if _SYSTEM_PROMPT_CACHE is None:
            prompt_path = os.path.join(os.path.dirname(__file__), "..", "..", "prompts", "reclassify.md")
            with open(prompt_path, "r") as file:
                _SYSTEM_PROMPT_CACHE = file.read()
        return _SYSTEM_PROMPT_CACHE

def count_tokens(text: str, model: str = "gpt-3.5-turbo") -> int:
    """Count tokens in text using tiktoken."""
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except KeyError:
        # Fallback to cl100k_base encoding for gpt-3.5-turbo
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))

def generate_llm_response_batch(conversations: List[Dict[str, Any]], batch_size: int = 5) -> List[Dict[str, Any]]:
    """
    Generate responses for multiple conversations in batches to reduce API calls.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    client = OpenAI(api_key=api_key)
    system_prompt = get_system_prompt(False)
    
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
        total_tokens = count_tokens(system_prompt + batched_prompt)
        print(f"Batch {i//batch_size + 1}: Processing {len(batch)} conversations, ~{total_tokens} tokens")
        
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

def get_conversations() -> List[Dict[str, Any]]:
    """Get conversations from the database."""
    conn = duckdb.connect('data/unthread_data.duckdb')
    with open('data/extract_for_summary.sql', 'r') as f:
        query = f.read()
    results = conn.execute(query).fetchall()
    
    # Convert to list of dictionaries
    conversations = []
    for row in results:
        conv = {
            'conversation_id': row[0],
            'ticket_type': row[13],
            'message_content': row[20]
        }
        conversations.append(conv)
    
    return conversations

def save_classifications_to_db(conversations: List[Dict[str, Any]], results: List[Dict[str, Any]]):
    """
    Save classification results to the database.
    """
    # Create connection to database
    conn = duckdb.connect('data/unthread_data.duckdb')
    
    # Create table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversation_classifications (
            conversation_id VARCHAR,
            category VARCHAR,
            sub_category VARCHAR,
            reasoning TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Prepare data for insertion
    classification_data = []
    for conversation, result in zip(conversations, results):
        if isinstance(result, dict) and 'error' not in result:
            classification_data.append((
                conversation['conversation_id'],
                result.get('category', 'Unknown'),
                result.get('sub_category', 'Unknown'),
                result.get('reasoning', '')
            ))
        else:
            # Handle error cases
            classification_data.append((
                conversation['conversation_id'],
                'Error',
                'Error',
                str(result) if result else 'Unknown error'
            ))
    
    # Insert data into table
    if classification_data:
        conn.executemany("""
            INSERT INTO conversation_classifications 
            (conversation_id, category, sub_category, reasoning)
            VALUES (?, ?, ?, ?)
        """, classification_data)
        
        print(f"Saved {len(classification_data)} classifications to database")
    
    conn.close()

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
    save_classifications_to_db(conversations, results)

if __name__ == "__main__":
    conversations = get_conversations()
    
    # Use batch processing for efficiency
    process_conversations_batch(conversations, batch_size=5, max_conversations=10)