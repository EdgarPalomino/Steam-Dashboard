from openai import OpenAI

# ---- Hardcoded API key (replace with your real key or an environment variable) ----
OPENAI_API_KEY = "YOUR_API_KEY_HERE"

# ---- Create client ----
client = OpenAI(api_key=OPENAI_API_KEY)

# ---- Test query ----
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You recommend Steam games based on gameplay history."},
        {"role": "user", "content": "Recommend one game, name only."}
    ],
    max_tokens=20,
    temperature=0.7
)

# ---- Print result ----
print("Response from OpenAI:")
print(response.choices[0].message.content.strip())
            