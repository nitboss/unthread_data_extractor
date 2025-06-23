import os
import openai

def generate_llm_response(conversation_data: dict, prompt: str) -> str:
    """
    Generate a response from OpenAI's LLM using the provided conversation data and prompt.
    """
    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    # Format the conversation data into a string
    conversation_text = """
    Conversation ID: 9b73dd2a-d390-4dc4-beb1-bf453a4d1dee
    Title: Sample Conversation
    Initial Message: Hello, how can I help you?
    Status: open
"""

    # Combine the prompt and conversation data
    full_prompt = f"{prompt}\n\n# Support case to be analyzed: \n]n <Converastion>{conversation_text}</Conversation>"

    # Read the system prompt from prompts/reclassify.md
    with open("prompts/reclassify.md", "r") as file:
        system_prompt = file.read()

    # Call OpenAI's API
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": full_prompt}
        ]
    )

    return response.choices[0].message.content

if __name__ == "__main__":
    # Example usage
    conversation_data = {
        "id": "9b73dd2a-d390-4dc4-beb1-bf453a4d1dee",
        "title": "Sample Conversation",
        "initialMessage": "Hello, how can I help you?",
        "status": "open"
    }
    prompt = "Summarize this conversation in one sentence."
    result = generate_llm_response(conversation_data, prompt)
    print(result) 