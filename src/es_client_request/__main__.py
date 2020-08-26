import sys
import asyncio
from src.es_client_request.client_request import main

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv))
