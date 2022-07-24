"""main.py is an async function that inits the database &s
extracts the logs from the source directory"""

from db import init, client
from extract import extractLog
from config import sourcedir
from logs import configureLogging


async def main():

    # Init database
    await init()

    # Extact logs from source directory
    await extractLog(sourcedir)


if __name__ == "__main__":

    configureLogging()

    loop = client.get_io_loop()
    loop.run_until_complete(main())
