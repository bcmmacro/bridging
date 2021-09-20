import logging

from app import App

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    app = App()
    app.run()
