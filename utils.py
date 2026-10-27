import re
import json
from decimal import Decimal
import time 

def clean_response(text: str) ->str:
    """Strip<thiking>..</thinking> tags form Nova model outputs."""

    return re.sub(r"thinking>.*?</thinking>\s*", "",str(text), flags = re.DOTALL).strip()

def run_agent_with_retry(agent_builder, prompt:str, max_retries:int = 3) ->float:
    """Run an agent with retry logic for transient Bedrock errors.
    Uses exponential backoff(1s, 2s,4s) to handle throttling."""

    for attempt in range(max_retries):
        try:
            agent = agent_builder()
            t= time.time()
            agent(prompt)
            return time.time() - t
        except Exception as e:
            if attempt < max_retries -1 :
                wait = 2 ** attempt
                print(f"[Retry {attempt +1} /{max_retries}] {e.__class__.__name__},waiting{wait}s")
                time.sleep(wait)
            else:
                print(f" [Failed]{e.__class__.__name__} after {max_retries} attempts")
                raise


