# API Context

## API Key Security Rules
- NEVER hardcode API keys in any file
- NEVER commit the .env file to Git (.gitignore must include .env)
- Always load keys from environment variables using python-dotenv
- The .env.example file is safe to commit — it shows key names only, no values

## How to Load Environment Variables

Always include this at the top of any file that uses the API:

```python
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("ANTHROPIC_API_KEY")
```

## Which Model to Use

```python
MODEL = "claude-sonnet-4-20250514"
```

Use this model for all Claude API calls in this project unless explicitly told otherwise.

## Standard API Call Pattern

```python
import anthropic
from dotenv import load_dotenv
import os

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": "Your prompt here"}
    ]
)

result = response.content[0].text
```

## Error Handling Rules for API Calls
- Always wrap API calls in try/except
- Catch `anthropic.APIError` for API-level failures
- Catch `anthropic.RateLimitError` separately and retry with backoff
- Log the error with the row/column context so failures are traceable
- Never silently swallow errors — always print or log what went wrong

```python
try:
    response = client.messages.create(...)
except anthropic.RateLimitError:
    print("Rate limit hit — waiting 10 seconds before retry")
    time.sleep(10)
    # retry logic here
except anthropic.APIError as e:
    print(f"API error: {e}")
    raise
```
