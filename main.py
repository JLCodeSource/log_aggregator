"""main.py is an async function that inits the database & 
extracts the logs from the source directory"""

from db import init, client
from extract import extractLog
from config import sourcedir
import logging


async def main():

    # Init database
    await init()

    # Extact logs from source directory
    await extractLog(sourcedir)


main()


if __name__ == "__main__":
    format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO, format=format)

    loop = client.get_io_loop()
    loop.run_until_complete(main())
