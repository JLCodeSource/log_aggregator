from db import init, client
from extract import extractLog
from vars import sourcedir


async def main():
    # Main app

    await init()

    await extractLog(sourcedir)


main()


if __name__ == "__main__":
    loop = client.get_io_loop()
    loop.run_until_complete(main())
