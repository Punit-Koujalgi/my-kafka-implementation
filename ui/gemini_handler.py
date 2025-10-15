import os
import openai

def generate_code(system_message, user_message) -> str:
    """Generate new Lox code using Gemini-Flash via OpenAI API"""
    try:
        # Initialize OpenAI client for Gemini-Flash
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            return "❌ Error: GEMINI_API_KEY environment variable not set"
        
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
        )
        
        # prompts = get_user_prompts_list()
        # selected_prompt = random.choice(prompts)

        response = client.chat.completions.create(
            model="gemini-2.5-flash-lite",
            messages=[
                {
                    "role": "system", 
                    "content": system_message
                },
                {
                    "role": "user", 
                    "content": user_message
                }
            ],
            max_tokens=800,
            temperature=0.7
        )
        
        llm_response = response.choices[0].message.content
        if not llm_response:
            return "❌ Error: No response from Gemini-Flash"
        
        return llm_response
        
    except Exception as e:
        return f"❌ Error generating code: {str(e)}"


